#![allow(dead_code)]

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamDoc {
    pub name: String,
    pub type_name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDoc {
    pub name: String,
    pub description: String,
    pub params: Vec<ParamDoc>,
    pub returns: String,
    pub visibility: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDoc {
    pub name: String,
    pub description: String,
    pub fields: Vec<ParamDoc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractDoc {
    pub contract_name: String,
    pub version: String,
    pub description: String,
    pub functions: Vec<FunctionDoc>,
    pub events: Vec<EventDoc>,
    pub generated_at: chrono::DateTime<Utc>,
}

pub struct DocGeneratorService {
    #[allow(dead_code)]
    db: PgPool,
}

impl DocGeneratorService {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    pub async fn generate(
        &self,
        contract_name: &str,
        source_code: &str,
    ) -> Result<ContractDoc, sqlx::Error> {
        let mut functions = Vec::new();
        let mut events = Vec::new();
        let mut description = String::new();
        let mut pending_doc = String::new();
        // Signature text of a `fn` whose header spans several lines.
        let mut pending_sig: Option<String> = None;

        for line in source_code.lines() {
            let trimmed = line.trim();
            if let Some(sig) = pending_sig.as_mut() {
                sig.push(' ');
                sig.push_str(trimmed);
                if signature_complete(sig) {
                    let sig = pending_sig.take().unwrap_or_default();
                    functions.extend(parse_function(&sig, pending_doc.trim()));
                    pending_doc.clear();
                } else if sig.len() > MAX_SIGNATURE_LEN {
                    // Unterminated header: give up rather than swallow the file.
                    pending_sig = None;
                    pending_doc.clear();
                }
            } else if let Some(doc) = trimmed.strip_prefix("///") {
                pending_doc.push_str(doc.trim());
                pending_doc.push(' ');
            } else if trimmed.starts_with("pub fn ") || trimmed.starts_with("fn ") {
                if signature_complete(trimmed) {
                    functions.extend(parse_function(trimmed, pending_doc.trim()));
                    pending_doc.clear();
                } else {
                    pending_sig = Some(trimmed.to_string());
                }
            } else if trimmed.contains("events().publish") {
                let event_name = parse_event_name(trimmed).unwrap_or_default();
                if !event_name.is_empty() {
                    events.push(EventDoc {
                        name: event_name,
                        description: pending_doc.trim().to_string(),
                        fields: vec![],
                    });
                }
                pending_doc.clear();
            } else if !trimmed.is_empty() {
                if description.is_empty() && !pending_doc.is_empty() {
                    description = pending_doc.trim().to_string();
                }
                pending_doc.clear();
            }
        }

        Ok(ContractDoc {
            contract_name: contract_name.to_string(),
            version: "0.1.0".to_string(),
            description,
            functions,
            events,
            generated_at: Utc::now(),
        })
    }

    pub fn generate_markdown(&self, doc: &ContractDoc) -> String {
        let mut md = format!("# {}\n\n", doc.contract_name);
        if !doc.description.is_empty() {
            md.push_str(&format!("{}", doc.description));
            md.push_str("\n\n");
        }
        if !doc.functions.is_empty() {
            md.push_str("## Functions\n\n");
            for f in &doc.functions {
                md.push_str(&format!("### `{}`\n\n", f.name));
                if !f.description.is_empty() {
                    md.push_str(&f.description);
                    md.push_str("\n\n");
                }
                if !f.params.is_empty() {
                    md.push_str("**Parameters:**\n\n");
                    for p in &f.params {
                        md.push_str(&format!("- `{}`: `{}`\n", p.name, p.type_name));
                    }
                    md.push('\n');
                }
                md.push_str(&format!("**Returns:** `{}`\n\n", f.returns));
            }
        }
        if !doc.events.is_empty() {
            md.push_str("## Events\n\n");
            for e in &doc.events {
                md.push_str(&format!("### `{}`\n\n{}", e.name, e.description));
                md.push_str("\n\n");
            }
        }
        md
    }

    pub fn generate_json(&self, doc: &ContractDoc) -> serde_json::Value {
        serde_json::to_value(doc).unwrap_or(serde_json::Value::Null)
    }
}

// ---------------------------------------------------------------------------
// Signature parsing
//
// Everything below works on untrusted source text: delimiters are matched
// explicitly, nesting depth is bounded, and malformed input degrades to the
// raw text instead of panicking.
// ---------------------------------------------------------------------------

/// Types nested deeper than this are rendered verbatim rather than parsed.
const MAX_TYPE_DEPTH: usize = 32;

/// Longest multi-line `fn` header collected before it is abandoned.
const MAX_SIGNATURE_LEN: usize = 8 * 1024;

/// A `fn` header is complete once its parameter list has closed and a body,
/// `;` or `where` clause has started.
fn signature_complete(sig: &str) -> bool {
    match sig.find('(').and_then(|open| matching_close(sig, open)) {
        Some(close) => {
            let rest = &sig[close + 1..];
            rest.contains('{') || rest.contains(';') || rest.contains(" where")
        }
        None => false,
    }
}

fn parse_function(sig: &str, description: &str) -> Option<FunctionDoc> {
    let visibility = if sig.starts_with("pub") { "public" } else { "private" };
    let after_fn = sig.split_once("fn ")?.1;

    let open = after_fn.find('(')?;
    let close = matching_close(after_fn, open)?;
    let name = after_fn[..open]
        .split('<')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if name.is_empty() {
        return None;
    }

    let params = split_top_level(&after_fn[open + 1..close], ',')
        .into_iter()
        .filter_map(|param| {
            let colon = find_top_level_colon(param)?;
            let param_name = param[..colon].trim();
            Some(ParamDoc {
                name: param_name.trim_start_matches("mut ").to_string(),
                type_name: format_type(&param[colon + 1..]),
                description: String::new(),
            })
        })
        .collect();

    let rest = after_fn[close + 1..].trim();
    let returns = match rest.strip_prefix("->") {
        Some(ret) => format_type(return_type_text(ret)),
        None => "()".to_string(),
    };

    Some(FunctionDoc {
        name,
        description: description.to_string(),
        params,
        returns,
        visibility: visibility.to_string(),
    })
}

/// The text of a return type, stopping at the body, `;` or `where` clause.
fn return_type_text(ret: &str) -> &str {
    let mut depth = 0usize;
    for (i, c) in ret.char_indices() {
        match c {
            '(' | '<' | '[' => depth += 1,
            ')' | '>' | ']' => depth = depth.saturating_sub(1),
            '{' | ';' if depth == 0 => return &ret[..i],
            _ if depth == 0 && ret[i..].starts_with(" where") => return &ret[..i],
            _ => {}
        }
    }
    ret
}

/// Extracts the event name from the first topic of `events().publish(...)`,
/// e.g. `publish((symbol_short!("transfer"), from), amount)` → `transfer`.
fn parse_event_name(line: &str) -> Option<String> {
    let start = line.find("publish(")? + "publish".len();
    let close = matching_close(line, start).unwrap_or(line.len());
    let mut topics = split_top_level(&line[start + 1..close], ',')
        .first()
        .copied()?
        .trim();
    // Unwrap any tuple layers to reach the first topic.
    for _ in 0..MAX_TYPE_DEPTH {
        let Some(inner) = topics.strip_prefix('(').and_then(|t| t.strip_suffix(')')) else {
            break;
        };
        topics = split_top_level(inner, ',').first().copied()?.trim();
    }

    let name = match (topics.find('"'), topics.rfind('"')) {
        (Some(a), Some(b)) if b > a => &topics[a + 1..b],
        _ => topics,
    };
    let name = name.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
    (!name.is_empty()).then(|| name.to_string())
}

/// Byte index of the delimiter closing the one at `open`, if balanced.
fn matching_close(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let (opener, closer) = match bytes.get(open)? {
        b'(' => (b'(', b')'),
        b'<' => (b'<', b'>'),
        b'[' => (b'[', b']'),
        _ => return None,
    };
    let mut depth = 0usize;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        if b == opener {
            depth += 1;
        } else if b == closer && !(closer == b'>' && i > 0 && bytes[i - 1] == b'-') {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

/// Splits on `sep` where it is not nested inside `()`, `<>` or `[]`.
fn split_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    let mut prev = '\0';
    for (i, c) in s.char_indices() {
        match c {
            '(' | '<' | '[' => depth += 1,
            '>' if prev == '-' => {}
            ')' | '>' | ']' => depth = depth.saturating_sub(1),
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
        prev = c;
    }
    parts.push(&s[start..]);
    parts.retain(|p| !p.trim().is_empty());
    parts
}

/// Position of the `name: Type` separator, skipping `::` path separators.
fn find_top_level_colon(param: &str) -> Option<usize> {
    let bytes = param.as_bytes();
    let mut depth = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' | b'<' | b'[' => depth += 1,
            b')' | b'>' | b']' => depth = depth.saturating_sub(1),
            b':' if depth == 0
                && bytes.get(i + 1) != Some(&b':')
                && (i == 0 || bytes[i - 1] != b':') =>
            {
                return Some(i);
            }
            _ => {}
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
enum TypeExpr {
    /// A path with optional generic arguments, e.g. `Vec<(u32, Symbol)>`.
    Path { name: String, args: Vec<TypeExpr> },
    /// A tuple; empty for `()`.
    Tuple(Vec<TypeExpr>),
    Reference { mutable: bool, inner: Box<TypeExpr> },
    /// `[T]` or `[T; N]`.
    Slice { inner: Box<TypeExpr>, len: Option<String> },
    /// Text that could not be parsed structurally.
    Raw(String),
}

impl std::fmt::Display for TypeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn join(f: &mut std::fmt::Formatter<'_>, items: &[TypeExpr]) -> std::fmt::Result {
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{item}")?;
            }
            Ok(())
        }

        match self {
            TypeExpr::Path { name, args } => {
                f.write_str(name)?;
                if !args.is_empty() {
                    f.write_str("<")?;
                    join(f, args)?;
                    f.write_str(">")?;
                }
                Ok(())
            }
            TypeExpr::Tuple(items) => {
                f.write_str("(")?;
                join(f, items)?;
                if items.len() == 1 {
                    f.write_str(",")?;
                }
                f.write_str(")")
            }
            TypeExpr::Reference { mutable, inner } => {
                write!(f, "&{}{inner}", if *mutable { "mut " } else { "" })
            }
            TypeExpr::Slice { inner, len: Some(len) } => write!(f, "[{inner}; {len}]"),
            TypeExpr::Slice { inner, len: None } => write!(f, "[{inner}]"),
            TypeExpr::Raw(text) => f.write_str(text),
        }
    }
}

/// Normalises a Rust type for display. Unparseable input is returned with
/// whitespace collapsed rather than rejected.
fn format_type(text: &str) -> String {
    let raw = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if raw.is_empty() {
        return "()".to_string();
    }
    let mut parser = TypeParser { src: &raw, pos: 0 };
    match parser.parse(0) {
        Some(ty) if parser.at_end() => ty.to_string(),
        _ => raw,
    }
}

struct TypeParser<'a> {
    src: &'a str,
    pos: usize,
}

impl TypeParser<'_> {
    fn rest(&self) -> &str {
        &self.src[self.pos..]
    }

    fn skip_ws(&mut self) {
        let trimmed = self.rest().trim_start();
        self.pos = self.src.len() - trimmed.len();
    }

    fn at_end(&mut self) -> bool {
        self.skip_ws();
        self.pos >= self.src.len()
    }

    fn eat(&mut self, token: &str) -> bool {
        self.skip_ws();
        if self.rest().starts_with(token) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    /// Comma-separated types up to `close`, allowing a trailing comma.
    /// Returns the items and whether any comma was seen.
    fn parse_list(&mut self, close: &str, depth: usize) -> Option<(Vec<TypeExpr>, bool)> {
        let mut items = Vec::new();
        let mut saw_comma = false;
        loop {
            if self.eat(close) {
                return Some((items, saw_comma));
            }
            items.push(self.parse(depth + 1)?);
            if self.eat(",") {
                saw_comma = true;
            } else if self.eat(close) {
                return Some((items, saw_comma));
            } else {
                return None;
            }
        }
    }

    fn parse(&mut self, depth: usize) -> Option<TypeExpr> {
        if depth > MAX_TYPE_DEPTH {
            // Too deep to format structurally: keep the remaining text as-is.
            let raw = self.rest().to_string();
            self.pos = self.src.len();
            return Some(TypeExpr::Raw(raw));
        }

        if self.eat("&") {
            // Skip an optional lifetime.
            self.skip_ws();
            if self.rest().starts_with('\'') {
                let end = self.rest().find(' ').unwrap_or(self.rest().len());
                self.pos += end;
            }
            let mutable = self.eat("mut ");
            let inner = Box::new(self.parse(depth + 1)?);
            return Some(TypeExpr::Reference { mutable, inner });
        }

        if self.eat("(") {
            let (items, saw_comma) = self.parse_list(")", depth)?;
            // `(T)` is just a parenthesised `T`, not a 1-tuple.
            if items.len() == 1 && !saw_comma {
                return items.into_iter().next();
            }
            return Some(TypeExpr::Tuple(items));
        }

        if self.eat("[") {
            let inner = Box::new(self.parse(depth + 1)?);
            let len = if self.eat(";") {
                let end = self.rest().find(']')?;
                let len = self.rest()[..end].trim().to_string();
                self.pos += end;
                Some(len)
            } else {
                None
            };
            if !self.eat("]") {
                return None;
            }
            return Some(TypeExpr::Slice { inner, len });
        }

        self.skip_ws();
        let rest = self.rest();
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':' || c == '\'' || c == '!'))
            .unwrap_or(rest.len());
        if end == 0 {
            return None;
        }
        let mut name = rest[..end].to_string();
        self.pos += end;

        // `impl Trait` / `dyn Trait`
        if (name == "impl" || name == "dyn") && !self.at_end() {
            let inner = self.parse(depth + 1)?;
            name = format!("{name} {inner}");
            return Some(TypeExpr::Path { name, args: vec![] });
        }

        let args = if self.eat("<") {
            self.parse_list(">", depth)?.0
        } else {
            Vec::new()
        };
        Some(TypeExpr::Path { name, args })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> DocGeneratorService {
        let db = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/crucible_test")
            .unwrap();
        DocGeneratorService::new(db)
    }

    #[test]
    fn format_type_handles_nested_unnamed_tuples() {
        assert_eq!(
            format_type("((u32,(i128 , (Symbol, Address))), Vec<(u64,(bool,))>)"),
            "((u32, (i128, (Symbol, Address))), Vec<(u64, (bool,))>)"
        );
        assert_eq!(format_type("()"), "()");
        assert_eq!(format_type("(u32,)"), "(u32,)");
        assert_eq!(format_type("Result<Map<Symbol, (u32, u32)>, Error>"), "Result<Map<Symbol, (u32, u32)>, Error>");
        assert_eq!(format_type("&'a mut [u8; 32]"), "&mut [u8; 32]");
    }

    #[test]
    fn format_type_survives_pathological_input() {
        let deep = format!("{}u32{}", "(".repeat(10_000), ",)".repeat(10_000));
        assert!(!format_type(&deep).is_empty());
        assert_eq!(format_type("((u32, i64"), "((u32, i64");
        assert_eq!(format_type("Vec<u32>>)"), "Vec<u32>>)");
    }

    #[tokio::test]
    async fn generate_parses_signatures_and_events() {
        let source = r#"
            /// Swaps two assets.
            pub fn swap(env: Env, pair: (Address, Address), amounts: (i128, (i128, u32))) -> Result<((i128, i128), Vec<(Symbol, (u32,))>), Error> {
                env.events().publish((symbol_short!("swap"), pair.0), amounts);
            }

            pub fn multi_line(
                env: Env,
                path: Vec<(Address, (u32, u64))>,
            ) -> Option<((u32, u32),)>
            where
                T: Clone,
            {
            }

            fn helper() {}
        "#;

        let doc = service().generate("Amm", source).await.unwrap();

        assert_eq!(doc.functions.len(), 3);
        let swap = &doc.functions[0];
        assert_eq!(swap.name, "swap");
        assert_eq!(swap.description, "Swaps two assets.");
        assert_eq!(swap.params.len(), 3);
        assert_eq!(swap.params[2].type_name, "(i128, (i128, u32))");
        assert_eq!(swap.returns, "Result<((i128, i128), Vec<(Symbol, (u32,))>), Error>");

        let multi = &doc.functions[1];
        assert_eq!(multi.params[1].type_name, "Vec<(Address, (u32, u64))>");
        assert_eq!(multi.returns, "Option<((u32, u32),)>");

        assert_eq!(doc.functions[2].returns, "()");
        assert_eq!(doc.functions[2].visibility, "private");

        assert_eq!(doc.events.len(), 1);
        assert_eq!(doc.events[0].name, "swap");

        let md = service().generate_markdown(&doc);
        assert!(md.contains("**Returns:** `Result<((i128, i128), Vec<(Symbol, (u32,))>), Error>`"));
    }
}
