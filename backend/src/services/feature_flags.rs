//! Feature flag service with Redis caching and PostgreSQL persistence.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use crate::services::tracing::TracingService;
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client as RedisClient};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing::{debug, info, instrument, warn, Instrument};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum FlagError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Feature flag not found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlag {
    pub key: String,
    pub enabled: bool,
    pub description: String,
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// FeatureFlagService
// ---------------------------------------------------------------------------

/// Upper bound on how long `is_enabled` waits for Redis before treating it as
/// unavailable and moving on to the next source.
const REDIS_TIMEOUT: Duration = Duration::from_millis(250);

pub struct FeatureFlagService {
    db: PgPool,
    redis: RedisClient,
    /// Last known value of every flag read or written by this process.
    /// Swapped atomically so readers never block during an outage.
    local: ArcSwap<HashMap<String, bool>>,
    /// Static values used when neither Redis, PostgreSQL nor the local
    /// snapshot can answer.
    defaults: HashMap<String, bool>,
}

impl FeatureFlagService {
    pub fn new(db: PgPool, redis: RedisClient) -> Self {
        Self::with_defaults(db, redis, HashMap::new())
    }

    pub fn with_defaults(db: PgPool, redis: RedisClient, defaults: HashMap<String, bool>) -> Self {
        Self {
            db,
            redis,
            local: ArcSwap::from_pointee(HashMap::new()),
            defaults,
        }
    }

    /// Resolves a flag from Redis, then PostgreSQL. Backend failures are
    /// logged and degrade to the local snapshot, then the static defaults;
    /// the backend error is only returned when no fallback value exists.
    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "is_enabled"))]
    pub async fn is_enabled(&self, key: &str) -> Result<bool, FlagError> {
        let cache_key = format!("flag:{key}");

        let mut conn = match self.redis_get(&cache_key).await {
            Ok((_, Some(val))) => {
                debug!(key = %key, "Feature flag cache hit");
                let enabled = val == "1";
                self.remember(key, Some(enabled));
                return Ok(enabled);
            }
            Ok((conn, None)) => Some(conn),
            Err(e) => {
                warn!(key = %key, error = %e, "Redis unavailable for feature flag lookup");
                None
            }
        };

        debug!(key = %key, "Feature flag cache miss – querying database");

        let db_span = TracingService::db_query_span(
            "SELECT enabled FROM feature_flags WHERE key = $1",
            "postgres",
            "SELECT",
        );
        let row: Result<Option<(bool,)>, sqlx::Error> =
            sqlx::query_as("SELECT enabled FROM feature_flags WHERE key = $1")
                .bind(key)
                .fetch_optional(&self.db)
                .instrument(db_span)
                .await;

        match row {
            Ok(Some((enabled,))) => {
                self.remember(key, Some(enabled));
                if let Some(conn) = conn.as_mut() {
                    let val = if enabled { "1" } else { "0" };
                    let cached: Result<(), _> = conn.set_ex(&cache_key, val, 300).await;
                    match cached {
                        Ok(()) => debug!(key = %key, enabled = enabled, "Cached feature flag"),
                        Err(e) => warn!(key = %key, error = %e, "Failed to cache feature flag"),
                    }
                }
                Ok(enabled)
            }
            Ok(None) => {
                self.remember(key, None);
                self.defaults
                    .get(key)
                    .copied()
                    .ok_or_else(|| FlagError::NotFound(key.to_string()))
            }
            Err(e) => {
                warn!(key = %key, error = %e, "Database unavailable for feature flag lookup");
                self.fallback(key).ok_or(FlagError::Database(e))
            }
        }
    }

    async fn redis_get(
        &self,
        cache_key: &str,
    ) -> Result<(MultiplexedConnection, Option<String>), FlagError> {
        let lookup = async {
            let mut conn = self.redis.get_multiplexed_async_connection().await?;
            let cached: Option<String> = conn.get(cache_key).await?;
            Ok::<_, FlagError>((conn, cached))
        }
        .instrument(TracingService::redis_command_span("GET", Some(cache_key)));

        tokio::time::timeout(REDIS_TIMEOUT, lookup)
            .await
            .map_err(|_| FlagError::Internal("Redis lookup timed out".to_string()))?
    }

    /// Last known value for `key`, else its static default.
    fn fallback(&self, key: &str) -> Option<bool> {
        self.local
            .load()
            .get(key)
            .copied()
            .or_else(|| self.defaults.get(key).copied())
    }

    /// Records (or forgets, for `None`) the last known value of a flag.
    fn remember(&self, key: &str, enabled: Option<bool>) {
        if self.local.load().get(key).copied() == enabled {
            return;
        }
        self.local.rcu(|current| {
            let mut next = HashMap::clone(current);
            match enabled {
                Some(enabled) => next.insert(key.to_string(), enabled),
                None => next.remove(key),
            };
            next
        });
    }

    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "get"))]
    pub async fn get(&self, key: &str) -> Result<FeatureFlag, FlagError> {
        let row: Option<(String, bool, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT key, enabled, description, updated_at FROM feature_flags WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(&self.db)
        .await?;

        match row {
            Some((key, enabled, description, updated_at)) => Ok(FeatureFlag {
                key,
                enabled,
                description,
                updated_at,
            }),
            None => Err(FlagError::NotFound(key.to_string())),
        }
    }

    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "list"))]
    pub async fn list(&self) -> Result<Vec<FeatureFlag>, FlagError> {
        let rows: Vec<(String, bool, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT key, enabled, description, updated_at FROM feature_flags ORDER BY key",
        )
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(key, enabled, description, updated_at)| FeatureFlag {
                key,
                enabled,
                description,
                updated_at,
            })
            .collect())
    }

    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "set"))]
    pub async fn set(&self, key: &str, enabled: bool, description: &str) -> Result<(), FlagError> {
        sqlx::query(
            r#"
            INSERT INTO feature_flags (key, enabled, description, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (key) DO UPDATE
            SET enabled = EXCLUDED.enabled,
                description = EXCLUDED.description,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(key)
        .bind(enabled)
        .bind(description)
        .bind(Utc::now())
        .execute(&self.db)
        .await?;

        self.remember(key, Some(enabled));
        self.invalidate_cache(key).await?;
        info!(key = %key, enabled = enabled, "Feature flag updated");
        Ok(())
    }

    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "delete"))]
    pub async fn delete(&self, key: &str) -> Result<(), FlagError> {
        let result = sqlx::query("DELETE FROM feature_flags WHERE key = $1")
            .bind(key)
            .execute(&self.db)
            .await?;

        if result.rows_affected() == 0 {
            return Err(FlagError::NotFound(key.to_string()));
        }

        self.remember(key, None);
        self.invalidate_cache(key).await?;
        info!(key = %key, "Feature flag deleted");
        Ok(())
    }

    async fn invalidate_cache(&self, key: &str) -> Result<(), FlagError> {
        let cache_key = format!("flag:{key}");
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let deleted: i32 = conn.del(&cache_key).await?;
        if deleted > 0 {
            debug!(key = %key, "Invalidated feature flag cache");
        } else {
            warn!(key = %key, "Cache key not found during invalidation");
        }
        Ok(())
    }

    #[instrument(skip(self), fields(service.name = "FeatureFlagService", service.method = "flush_cache"))]
    pub async fn flush_cache(&self) -> Result<usize, FlagError> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("flag:*")
            .query_async(&mut conn)
            .await?;

        if keys.is_empty() {
            return Ok(0);
        }

        let count = keys.len();
        for key in keys {
            let _: () = conn.del(&key).await?;
        }

        info!(count = count, "Flushed feature flag cache");
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flag_error_display() {
        let err = FlagError::NotFound("test_flag".to_string());
        assert!(err.to_string().contains("test_flag"));
    }

    #[test]
    fn test_feature_flag_serialization() {
        let flag = FeatureFlag {
            key: "test".to_string(),
            enabled: true,
            description: "Test flag".to_string(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&flag).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("true"));
    }

    #[test]
    fn test_feature_flag_deserialization() {
        let json = r#"{
            "key": "beta",
            "enabled": false,
            "description": "Beta features",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;
        let flag: FeatureFlag = serde_json::from_str(json).unwrap();
        assert_eq!(flag.key, "beta");
        assert!(!flag.enabled);
    }

    /// Service whose Redis and PostgreSQL both refuse connections.
    fn offline_service(defaults: HashMap<String, bool>) -> FeatureFlagService {
        let db = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/crucible_test")
            .unwrap();
        let redis = RedisClient::open("redis://127.0.0.1:1/").unwrap();
        FeatureFlagService::with_defaults(db, redis, defaults)
    }

    #[tokio::test]
    async fn is_enabled_falls_back_to_static_default_when_backends_are_down() {
        let service = offline_service(HashMap::from([("beta".to_string(), true)]));
        assert!(service.is_enabled("beta").await.unwrap());
    }

    #[tokio::test]
    async fn is_enabled_prefers_last_known_value_over_default() {
        let service = offline_service(HashMap::from([("beta".to_string(), true)]));
        service.remember("beta", Some(false));
        assert!(!service.is_enabled("beta").await.unwrap());
    }

    #[tokio::test]
    async fn is_enabled_errors_when_backends_are_down_and_no_fallback_exists() {
        let service = offline_service(HashMap::new());
        assert!(matches!(
            service.is_enabled("unknown").await,
            Err(FlagError::Database(_))
        ));
    }
}
