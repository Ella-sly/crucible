//! Build request deduplication service.
//!
//! The service protects expensive build submission paths from repeated work by
//! deriving a stable SHA-256 fingerprint from the request identity, acquiring a
//! short-lived Redis lock with `SET NX EX`, and optionally persisting the
//! deduplication record in PostgreSQL. Workers keep the lock alive for long
//! compilations with [`BuildDedupService::spawn_lock_heartbeat`].

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

/// Default Redis namespace for build request deduplication keys.
pub const DEFAULT_NAMESPACE: &str = "build:dedup";

/// Configuration for deduplication key retention and queueing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupConfig {
    /// Redis key namespace.
    pub namespace: String,
    /// Completed duplicate response TTL in seconds.
    pub result_ttl_seconds: usize,
    /// In-flight lock TTL in seconds.
    pub lock_ttl_seconds: usize,
    /// Redis list used to enqueue accepted build requests.
    pub queue_name: String,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
            result_ttl_seconds: 86_400,
            lock_ttl_seconds: 900,
            queue_name: "build:requests".to_string(),
        }
    }
}

/// A build request payload eligible for deduplication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuildRequest {
    /// HTTP method or logical command name.
    pub method: String,
    /// HTTP path or logical build target.
    pub path: String,
    /// Optional caller identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester_id: Option<String>,
    /// Optional client supplied idempotency key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Request body.
    #[serde(default)]
    pub body: Value,
}

impl BuildRequest {
    /// Creates a request value from route parts and JSON body.
    pub fn new(method: impl Into<String>, path: impl Into<String>, body: Value) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            requester_id: None,
            idempotency_key: None,
            body,
        }
    }

    /// Sets the requester identity.
    pub fn requester_id(mut self, requester_id: impl Into<String>) -> Self {
        self.requester_id = Some(requester_id.into());
        self
    }

    /// Sets the client supplied idempotency key.
    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    fn validate(&self) -> Result<(), DedupError> {
        if self.method.trim().is_empty() {
            return Err(DedupError::InvalidRequest("method is required".to_string()));
        }
        if self.path.trim().is_empty() {
            return Err(DedupError::InvalidRequest("path is required".to_string()));
        }
        Ok(())
    }
}

/// Deduplication state for a submitted build request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DedupStatus {
    /// The request is new and was accepted for build processing.
    Accepted,
    /// The same request is already being processed.
    InProgress,
    /// The same request was already accepted or completed.
    Duplicate,
}

/// Deduplication decision returned to callers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DedupDecision {
    /// Stable request fingerprint.
    pub fingerprint: String,
    /// Server generated request id for the accepted or existing request.
    pub request_id: Uuid,
    /// Decision status.
    pub status: DedupStatus,
    /// Whether a cached result is available.
    pub replayable: bool,
}

impl DedupDecision {
    fn accepted(fingerprint: String, request_id: Uuid) -> Self {
        Self {
            fingerprint,
            request_id,
            status: DedupStatus::Accepted,
            replayable: false,
        }
    }

    fn duplicate(fingerprint: String, request_id: Uuid, replayable: bool) -> Self {
        Self {
            fingerprint,
            request_id,
            status: DedupStatus::Duplicate,
            replayable,
        }
    }

    fn in_progress(fingerprint: String, request_id: Uuid) -> Self {
        Self {
            fingerprint,
            request_id,
            status: DedupStatus::InProgress,
            replayable: false,
        }
    }
}

/// Errors produced by the build request deduplication service.
#[derive(Debug, Error)]
pub enum DedupError {
    /// The request did not contain enough data to be fingerprinted safely.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// JSON serialization failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Redis operation failed.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    /// PostgreSQL operation failed.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for DedupError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::Serialization(_) | Self::Redis(_) | Self::Database(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };

        (
            status,
            Json(json!({
                "error": self.to_string(),
                "code": status.as_u16()
            })),
        )
            .into_response()
    }
}

/// Production build request deduplicator.
#[derive(Clone)]
pub struct BuildDedupService {
    db: PgPool,
    redis: redis::Client,
    config: DedupConfig,
}

impl BuildDedupService {
    /// Creates a deduplication service.
    pub fn new(db: PgPool, redis: redis::Client) -> Self {
        Self::with_config(db, redis, DedupConfig::default())
    }

    /// Creates a deduplication service with explicit configuration.
    pub fn with_config(db: PgPool, redis: redis::Client, config: DedupConfig) -> Self {
        Self { db, redis, config }
    }

    /// Creates the persistence table required by this service.
    ///
    /// This mirrors `backend/migrations/20260528010000_build_request_dedup.sql`
    /// for isolated integration tests. Production deployments should apply the
    /// migration through the normal SQLx migration path.
    pub async fn ensure_schema(pool: &PgPool) -> Result<(), DedupError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS build_request_dedup (
                request_id UUID PRIMARY KEY,
                fingerprint TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                request_payload JSONB NOT NULL,
                response_payload JSONB,
                duplicate_count BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL,
                CONSTRAINT build_request_dedup_status_check
                    CHECK (status IN ('accepted', 'completed', 'failed'))
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_build_request_dedup_expires_at
                ON build_request_dedup (expires_at)
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_build_request_dedup_status
                ON build_request_dedup (status)
            "#,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Computes the stable request fingerprint.
    pub fn fingerprint(request: &BuildRequest) -> Result<String, DedupError> {
        request.validate()?;

        let canonical = serde_json::to_vec(&json!({
            "method": request.method.trim().to_ascii_uppercase(),
            "path": request.path.trim(),
            "requester_id": &request.requester_id,
            "idempotency_key": &request.idempotency_key,
            "body": &request.body,
        }))?;

        let mut hasher = Sha256::new();
        hasher.update((canonical.len() as u64).to_be_bytes());
        hasher.update(canonical);
        Ok(to_hex(&hasher.finalize()))
    }

    /// Checks and registers a build request.
    ///
    /// Time complexity is O(n) for JSON serialization plus O(1) Redis and
    /// indexed PostgreSQL operations. Space complexity is O(n), where n is the
    /// serialized request size.
    #[instrument(skip(self, request), fields(path = %request.path, method = %request.method))]
    pub async fn check_or_register(
        &self,
        request: BuildRequest,
    ) -> Result<DedupDecision, DedupError> {
        let fingerprint = Self::fingerprint(&request)?;
        let request_id = Uuid::new_v4();
        let lock_key = self.lock_key(&fingerprint);
        let result_key = self.result_key(&fingerprint);
        let payload = serde_json::to_string(&QueuedBuildRequest {
            request_id,
            fingerprint: fingerprint.clone(),
            request: request.clone(),
            queued_at: Utc::now(),
        })?;

        let mut conn = self.redis.get_multiplexed_async_connection().await?;

        if let Some(existing) = conn.get::<_, Option<String>>(&result_key).await? {
            if let Ok(existing_id) = Uuid::parse_str(&existing) {
                debug!(%fingerprint, %existing_id, "dedup cache hit");
                return Ok(DedupDecision::duplicate(fingerprint, existing_id, true));
            }
        }

        let lock_acquired: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(request_id.to_string())
            .arg("NX")
            .arg("EX")
            .arg(self.config.lock_ttl_seconds)
            .query_async(&mut conn)
            .await?;

        if lock_acquired.is_none() {
            let existing_id = conn
                .get::<_, Option<String>>(&lock_key)
                .await?
                .and_then(|id| Uuid::parse_str(&id).ok())
                .unwrap_or(request_id);
            warn!(%fingerprint, %existing_id, "duplicate build request is already in progress");
            return Ok(DedupDecision::in_progress(fingerprint, existing_id));
        }

        let persisted = self
            .persist_request(&fingerprint, request_id, &request)
            .await?;
        if persisted.request_id != request_id {
            self.release_lock(&lock_key, request_id).await?;
            return Ok(DedupDecision::duplicate(
                fingerprint,
                persisted.request_id,
                persisted.status == "completed",
            ));
        }

        conn.rpush::<_, _, usize>(&self.config.queue_name, payload)
            .await?;
        info!(%fingerprint, %request_id, "accepted unique build request");

        Ok(DedupDecision::accepted(fingerprint, request_id))
    }

    /// Marks a request completed and stores the replay id in Redis.
    #[instrument(skip(self, response))]
    pub async fn complete(
        &self,
        fingerprint: &str,
        request_id: Uuid,
        response: Value,
    ) -> Result<(), DedupError> {
        sqlx::query(
            r#"
            UPDATE build_request_dedup
            SET status = 'completed',
                response_payload = $3,
                updated_at = NOW()
            WHERE fingerprint = $1 AND request_id = $2
            "#,
        )
        .bind(fingerprint)
        .bind(request_id)
        .bind(response)
        .execute(&self.db)
        .await?;

        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        conn.set_ex::<_, _, ()>(
            self.result_key(fingerprint),
            request_id.to_string(),
            self.config.result_ttl_seconds as u64,
        )
        .await?;
        self.release_lock(&self.lock_key(fingerprint), request_id)
            .await?;
        Ok(())
    }

    /// Marks a request failed and releases the in-flight lock.
    #[instrument(skip(self))]
    pub async fn fail(&self, fingerprint: &str, request_id: Uuid) -> Result<(), DedupError> {
        sqlx::query(
            r#"
            UPDATE build_request_dedup
            SET status = 'failed', updated_at = NOW()
            WHERE fingerprint = $1 AND request_id = $2
            "#,
        )
        .bind(fingerprint)
        .bind(request_id)
        .execute(&self.db)
        .await?;

        self.release_lock(&self.lock_key(fingerprint), request_id)
            .await?;
        Ok(())
    }

    /// Resets the in-flight lock TTL if `request_id` still owns it.
    ///
    /// Returns `false` when the lock has expired or belongs to another
    /// request, in which case the caller no longer holds deduplication.
    pub async fn extend_lock(
        &self,
        fingerprint: &str,
        request_id: Uuid,
    ) -> Result<bool, DedupError> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let extended = redis::Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("EXPIRE", KEYS[1], ARGV[2])
            end
            return 0
            "#,
        )
        .key(self.lock_key(fingerprint))
        .arg(request_id.to_string())
        .arg(self.config.lock_ttl_seconds)
        .invoke_async::<i32>(&mut conn)
        .await?;
        Ok(extended == 1)
    }

    /// Keeps the in-flight lock alive for as long as the returned guard lives.
    ///
    /// Build workers should hold the guard for the full compilation so that
    /// builds outliving `lock_ttl_seconds` are not compiled a second time.
    /// The lock is renewed every third of its TTL; dropping the guard stops
    /// renewal without releasing the lock (use `complete` or `fail` for that).
    pub fn spawn_lock_heartbeat(&self, fingerprint: &str, request_id: Uuid) -> LockHeartbeat {
        let service = self.clone();
        let fingerprint = fingerprint.to_string();
        let interval = heartbeat_interval(self.config.lock_ttl_seconds);

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match service.extend_lock(&fingerprint, request_id).await {
                    Ok(true) => debug!(%fingerprint, %request_id, "renewed build dedup lock"),
                    Ok(false) => {
                        warn!(%fingerprint, %request_id, "build dedup lock lost; stopping heartbeat");
                        return;
                    }
                    // Transient Redis failure: retry on the next tick while the
                    // remaining TTL still covers us.
                    Err(e) => warn!(%fingerprint, %request_id, error = %e, "failed to renew build dedup lock"),
                }
            }
        });

        LockHeartbeat { task }
    }

    fn lock_key(&self, fingerprint: &str) -> String {
        format!("{}:{}:lock", self.config.namespace, fingerprint)
    }

    fn result_key(&self, fingerprint: &str) -> String {
        format!("{}:{}:result", self.config.namespace, fingerprint)
    }

    async fn persist_request(
        &self,
        fingerprint: &str,
        request_id: Uuid,
        request: &BuildRequest,
    ) -> Result<PersistedDedupRecord, DedupError> {
        let expires_at =
            Utc::now() + chrono::Duration::seconds(self.config.result_ttl_seconds as i64);
        let payload = serde_json::to_value(request)?;

        let row = sqlx::query_as::<_, PersistedDedupRecord>(
            r#"
            INSERT INTO build_request_dedup (
                request_id,
                fingerprint,
                status,
                request_payload,
                expires_at
            )
            VALUES ($1, $2, 'accepted', $3, $4)
            ON CONFLICT (fingerprint) DO UPDATE
            SET duplicate_count = build_request_dedup.duplicate_count + 1,
                updated_at = NOW()
            RETURNING request_id, status
            "#,
        )
        .bind(request_id)
        .bind(fingerprint)
        .bind(payload)
        .bind(expires_at)
        .fetch_one(&self.db)
        .await?;

        Ok(row)
    }

    async fn release_lock(&self, lock_key: &str, request_id: Uuid) -> Result<(), DedupError> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        redis::Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            end
            return 0
            "#,
        )
        .key(lock_key)
        .arg(request_id.to_string())
        .invoke_async::<i32>(&mut conn)
        .await?;
        Ok(())
    }
}

/// Guard returned by [`BuildDedupService::spawn_lock_heartbeat`]; renewal
/// stops when it is dropped.
#[derive(Debug)]
pub struct LockHeartbeat {
    task: tokio::task::JoinHandle<()>,
}

impl LockHeartbeat {
    /// Whether the heartbeat stopped on its own because the lock was lost.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

impl Drop for LockHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Renewal period for a lock with the given TTL: a third of the TTL, so two
/// consecutive renewals can fail before the lock expires.
fn heartbeat_interval(lock_ttl_seconds: usize) -> Duration {
    Duration::from_millis((lock_ttl_seconds as u64 * 1000 / 3).max(1000))
}

/// Shared Axum state for deduplicated build request routes.
#[derive(Clone)]
pub struct DedupState {
    /// Deduplication service.
    pub service: Arc<BuildDedupService>,
}

/// Builds an Axum router for deduplicated build submissions.
pub fn router(state: DedupState) -> Router {
    Router::new()
        .route("/build-requests", post(submit_build_request))
        .with_state(state)
}

/// Axum handler for a deduplicated build request submission.
pub async fn submit_build_request(
    State(state): State<DedupState>,
    Json(request): Json<BuildRequest>,
) -> Result<Json<DedupDecision>, DedupError> {
    state.service.check_or_register(request).await.map(Json)
}

#[derive(Debug, Serialize)]
struct QueuedBuildRequest {
    request_id: Uuid,
    fingerprint: String,
    request: BuildRequest,
    queued_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct PersistedDedupRecord {
    request_id: Uuid,
    status: String,
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn sample_request() -> BuildRequest {
        BuildRequest::new(
            "post",
            "/api/build",
            json!({
                "repo": "crucible",
                "target": "backend",
                "commit": "abc123"
            }),
        )
        .requester_id("user-1")
    }

    #[test]
    fn fingerprint_is_stable_for_same_request() {
        let first = BuildDedupService::fingerprint(&sample_request()).unwrap();
        let second = BuildDedupService::fingerprint(&sample_request()).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn fingerprint_changes_when_body_changes() {
        let first = BuildDedupService::fingerprint(&sample_request()).unwrap();
        let changed = BuildRequest::new(
            "post",
            "/api/build",
            json!({
                "repo": "crucible",
                "target": "contracts",
                "commit": "abc123"
            }),
        )
        .requester_id("user-1");

        let second = BuildDedupService::fingerprint(&changed).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn fingerprint_normalizes_method_and_path_whitespace() {
        let first = BuildDedupService::fingerprint(&sample_request()).unwrap();
        let second = BuildDedupService::fingerprint(&BuildRequest {
            method: " POST ".to_string(),
            path: " /api/build ".to_string(),
            ..sample_request()
        })
        .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn empty_method_is_rejected() {
        let err = BuildDedupService::fingerprint(&BuildRequest::new("", "/api/build", json!({})))
            .unwrap_err();

        assert!(matches!(err, DedupError::InvalidRequest(_)));
    }

    #[test]
    fn dedup_config_defaults_are_bounded() {
        let config = DedupConfig::default();

        assert_eq!(config.namespace, DEFAULT_NAMESPACE);
        assert!(config.lock_ttl_seconds > 0);
        assert!(config.lock_ttl_seconds < config.result_ttl_seconds);
        assert!(!config.queue_name.is_empty());
    }

    #[test]
    fn heartbeat_renews_well_before_lock_expiry() {
        assert_eq!(heartbeat_interval(30), Duration::from_secs(10));
        assert_eq!(heartbeat_interval(900), Duration::from_secs(300));
        assert_eq!(heartbeat_interval(1), Duration::from_secs(1));
    }

    #[test]
    fn handler_router_can_be_constructed() {
        let db = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/crucible_test")
            .unwrap();
        let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
        let state = DedupState {
            service: Arc::new(BuildDedupService::new(db, redis)),
        };

        let _router = router(state);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL and Redis test services"]
    async fn check_or_register_accepts_then_deduplicates_request() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/crucible_test".to_string());
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let db = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        BuildDedupService::ensure_schema(&db).await.unwrap();

        let redis = redis::Client::open(redis_url).unwrap();
        let config = DedupConfig {
            namespace: format!("{}:test:{}", DEFAULT_NAMESPACE, Uuid::new_v4()),
            result_ttl_seconds: 60,
            lock_ttl_seconds: 30,
            queue_name: format!("build:requests:test:{}", Uuid::new_v4()),
        };
        let service = BuildDedupService::with_config(db, redis, config);
        let request = sample_request().idempotency_key("ci-build-1");

        let accepted = service.check_or_register(request.clone()).await.unwrap();
        let duplicate = service.check_or_register(request).await.unwrap();

        assert_eq!(accepted.status, DedupStatus::Accepted);
        assert_eq!(duplicate.status, DedupStatus::InProgress);
        assert_eq!(accepted.request_id, duplicate.request_id);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL and Redis test services"]
    async fn complete_marks_request_replayable_for_duplicates() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/crucible_test".to_string());
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let db = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        BuildDedupService::ensure_schema(&db).await.unwrap();

        let redis = redis::Client::open(redis_url).unwrap();
        let config = DedupConfig {
            namespace: format!("{}:test:{}", DEFAULT_NAMESPACE, Uuid::new_v4()),
            result_ttl_seconds: 60,
            lock_ttl_seconds: 30,
            queue_name: format!("build:requests:test:{}", Uuid::new_v4()),
        };
        let service = BuildDedupService::with_config(db, redis, config);
        let request = sample_request().idempotency_key("ci-build-2");

        let accepted = service.check_or_register(request.clone()).await.unwrap();
        service
            .complete(
                &accepted.fingerprint,
                accepted.request_id,
                json!({ "build_id": "build-1" }),
            )
            .await
            .unwrap();
        let duplicate = service.check_or_register(request).await.unwrap();

        assert_eq!(duplicate.status, DedupStatus::Duplicate);
        assert_eq!(duplicate.request_id, accepted.request_id);
        assert!(duplicate.replayable);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL and Redis test services"]
    async fn heartbeat_keeps_lock_alive_past_its_ttl() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/crucible_test".to_string());
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let db = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        BuildDedupService::ensure_schema(&db).await.unwrap();

        let redis = redis::Client::open(redis_url).unwrap();
        let config = DedupConfig {
            namespace: format!("{}:test:{}", DEFAULT_NAMESPACE, Uuid::new_v4()),
            result_ttl_seconds: 60,
            lock_ttl_seconds: 3,
            queue_name: format!("build:requests:test:{}", Uuid::new_v4()),
        };
        let service = BuildDedupService::with_config(db, redis, config);
        let request = sample_request().idempotency_key(Uuid::new_v4().to_string());

        let accepted = service.check_or_register(request.clone()).await.unwrap();
        let heartbeat = service.spawn_lock_heartbeat(&accepted.fingerprint, accepted.request_id);
        tokio::time::sleep(Duration::from_secs(7)).await;

        let duplicate = service.check_or_register(request).await.unwrap();
        assert_eq!(duplicate.status, DedupStatus::InProgress);
        assert_eq!(duplicate.request_id, accepted.request_id);
        assert!(!heartbeat.is_finished());
    }
}
