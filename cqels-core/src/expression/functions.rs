//! SPARQL 1.1 built-in function implementations.
//!
//! Dispatches function calls by name (case-insensitive) to implementations
//! following SPARQL semantics: null input produces null output.

use std::time::{SystemTime, UNIX_EPOCH};

use cqels_model::term::{BlankNodeTerm, IriTerm};
use cqels_model::{Term, Value};

/// Evaluates a built-in function by name.
///
/// Returns `Value::Null` for unknown functions or when null propagation applies.
pub fn call_builtin(name: &str, args: &[Value]) -> Value {
    match name.to_lowercase().as_str() {
        // ── Numeric ──────────────────────────────────────────────────────
        "abs" => numeric_unary(args, f64::abs),
        "ceil" => numeric_unary(args, f64::ceil),
        "floor" => numeric_unary(args, f64::floor),
        "round" => numeric_unary(args, f64::round),
        "rand" => Value::Float(pseudo_random()),
        "power" | "pow" => {
            match (
                args.first().and_then(|v| v.as_numeric()),
                args.get(1).and_then(|v| v.as_numeric()),
            ) {
                (Some(base), Some(exp)) => Value::Float(base.powf(exp)),
                _ => Value::Null,
            }
        }

        // ── String ───────────────────────────────────────────────────────
        "concat" => fn_concat(args),
        "strlen" => fn_strlen(args),
        "substr" | "substring" => fn_substr(args),
        "ucase" => fn_ucase(args),
        "lcase" => fn_lcase(args),
        "contains" => fn_contains(args),
        "strstarts" => fn_strstarts(args),
        "strends" => fn_strends(args),
        "str" => fn_str(args),
        "replace" => fn_replace(args),
        "encode_for_uri" | "encode" => fn_encode_for_uri(args),

        // ── DateTime ─────────────────────────────────────────────────────
        "year" => fn_year(args),
        "month" => fn_month(args),
        "day" => fn_day(args),
        "hours" => fn_hours(args),
        "minutes" => fn_minutes(args),
        "seconds" => fn_seconds(args),
        "now" => fn_now(),

        // ── Type testing ─────────────────────────────────────────────────
        "isiri" | "isuri" => fn_isiri(args),
        "isblank" => fn_isblank(args),
        "isliteral" => fn_isliteral(args),
        "isnumeric" => fn_isnumeric(args),
        "bound" => fn_bound(args),
        "datatype" => fn_datatype(args),
        "lang" => fn_lang(args),

        // ── Constructors ─────────────────────────────────────────────────
        "iri" | "uri" => fn_iri(args),
        "bnode" => fn_bnode(args),
        "uuid" => fn_uuid(),
        "struuid" => fn_struuid(),

        // ── Casting ──────────────────────────────────────────────────────
        "integer" | "xsd:integer" => fn_to_integer(args),
        "float" | "double" | "xsd:double" | "xsd:float" => fn_to_float(args),
        "boolean" | "xsd:boolean" => fn_to_boolean(args),
        "string" | "xsd:string" => fn_str(args),

        // ── Hash ─────────────────────────────────────────────────────────
        "md5" => fn_hash::<md5::Md5>(args),
        "sha1" => fn_hash::<sha1::Sha1>(args),
        "sha256" => fn_hash::<sha2::Sha256>(args),
        "sha384" => fn_hash::<sha2::Sha384>(args),
        "sha512" => fn_hash::<sha2::Sha512>(args),

        // ── Coalesce ─────────────────────────────────────────────────────
        "coalesce" => {
            for arg in args {
                if !arg.is_null() {
                    return arg.clone();
                }
            }
            Value::Null
        }

        // ── IF (also handled at expression level) ────────────────────────
        "if" => {
            if args.len() >= 3 {
                let cond = value_to_bool(&args[0]);
                if cond {
                    args[1].clone()
                } else {
                    args[2].clone()
                }
            } else {
                Value::Null
            }
        }

        _ => Value::Null,
    }
}

// ── Helper: apply a unary numeric function ──────────────────────────────────

fn numeric_unary(args: &[Value], f: fn(f64) -> f64) -> Value {
    match args.first() {
        Some(v) => match v.as_numeric() {
            Some(n) => {
                let result = f(n);
                // Preserve integer type when possible
                if matches!(v, Value::Integer(_)) && result.fract() == 0.0 {
                    Value::Integer(result as i64)
                } else {
                    Value::Float(result)
                }
            }
            None => Value::Null,
        },
        None => Value::Null,
    }
}

// ── String functions ────────────────────────────────────────────────────────

fn fn_concat(args: &[Value]) -> Value {
    let mut result = String::new();
    for arg in args {
        match arg.as_string() {
            Some(s) => result.push_str(s),
            None => return Value::Null,
        }
    }
    Value::String(result)
}

fn fn_strlen(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => Value::Integer(s.chars().count() as i64),
        None => Value::Null,
    }
}

fn fn_substr(args: &[Value]) -> Value {
    let s = match args.first().and_then(|v| v.as_string()) {
        Some(s) => s,
        None => return Value::Null,
    };
    // SPARQL SUBSTR is 1-based. Per spec, the effective range is
    // [max(startingLoc, 1), startingLoc + length) intersected with [1, strlen+1).
    let start_loc = match args.get(1).and_then(|v| v.as_integer()) {
        Some(i) => i,
        None => return Value::Null,
    };
    let chars: Vec<char> = s.chars().collect();
    match args.get(2).and_then(|v| v.as_integer()) {
        Some(length) => {
            // Effective start: max(startingLoc, 1), converted to 0-based
            let eff_start = (start_loc.max(1) - 1) as usize;
            // Effective end: startingLoc + length - 1, converted to 0-based
            let eff_end = ((start_loc + length - 1).max(0) as usize).min(chars.len());
            if eff_start >= chars.len() || eff_start >= eff_end {
                Value::String(String::new())
            } else {
                Value::String(chars[eff_start..eff_end].iter().collect())
            }
        }
        None => {
            let eff_start = (start_loc.max(1) - 1) as usize;
            if eff_start >= chars.len() {
                Value::String(String::new())
            } else {
                Value::String(chars[eff_start..].iter().collect())
            }
        }
    }
}

fn fn_ucase(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => Value::String(s.to_uppercase()),
        None => Value::Null,
    }
}

fn fn_lcase(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => Value::String(s.to_lowercase()),
        None => Value::Null,
    }
}

fn fn_contains(args: &[Value]) -> Value {
    match (
        args.first().and_then(|v| v.as_string()),
        args.get(1).and_then(|v| v.as_string()),
    ) {
        (Some(haystack), Some(needle)) => Value::Boolean(haystack.contains(needle)),
        _ => Value::Null,
    }
}

fn fn_strstarts(args: &[Value]) -> Value {
    match (
        args.first().and_then(|v| v.as_string()),
        args.get(1).and_then(|v| v.as_string()),
    ) {
        (Some(s), Some(prefix)) => Value::Boolean(s.starts_with(prefix)),
        _ => Value::Null,
    }
}

fn fn_strends(args: &[Value]) -> Value {
    match (
        args.first().and_then(|v| v.as_string()),
        args.get(1).and_then(|v| v.as_string()),
    ) {
        (Some(s), Some(suffix)) => Value::Boolean(s.ends_with(suffix)),
        _ => Value::Null,
    }
}

fn fn_str(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Null) => Value::Null,
        Some(v) => Value::String(v.to_string()),
        None => Value::Null,
    }
}

fn fn_replace(args: &[Value]) -> Value {
    match (
        args.first().and_then(|v| v.as_string()),
        args.get(1).and_then(|v| v.as_string()),
        args.get(2).and_then(|v| v.as_string()),
    ) {
        (Some(input), Some(pattern), Some(replacement)) => {
            // Try regex first (SPARQL spec), fall back to literal replacement
            match regex::Regex::new(pattern) {
                Ok(re) => Value::String(re.replace_all(input, replacement as &str).into_owned()),
                Err(_) => Value::String(input.replace(pattern, replacement)),
            }
        }
        _ => Value::Null,
    }
}

fn fn_encode_for_uri(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => {
            let mut encoded = String::new();
            for c in s.chars() {
                if c.is_ascii_alphanumeric() || "-._~".contains(c) {
                    encoded.push(c);
                } else {
                    // Percent-encode each UTF-8 byte, not the codepoint
                    let mut buf = [0u8; 4];
                    for byte in c.encode_utf8(&mut buf).as_bytes() {
                        use std::fmt::Write;
                        write!(encoded, "%{:02X}", byte).unwrap();
                    }
                }
            }
            Value::String(encoded)
        }
        None => Value::Null,
    }
}

// ── DateTime functions (basic extraction from ISO-8601 strings) ─────────────

fn extract_datetime_part(args: &[Value], part_index: usize) -> Value {
    let s = match args.first().and_then(|v| v.as_string()) {
        Some(s) => s.to_string(),
        None => return Value::Null,
    };
    // Parse ISO-8601: YYYY-MM-DDThh:mm:ss[.frac][Z|+HH:MM|-HH:MM]
    // Split date and time parts carefully to avoid timezone offset interference
    let (date_part, time_part) = match s.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (s.as_str(), None),
    };

    let date_fields: Vec<&str> = date_part.splitn(3, '-').collect();

    match part_index {
        // Year, Month, Day from date part
        0 => date_fields
            .first()
            .and_then(|p| p.parse::<i64>().ok())
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        1 => date_fields
            .get(1)
            .and_then(|p| p.parse::<i64>().ok())
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        2 => date_fields
            .get(2)
            .and_then(|p| p.parse::<i64>().ok())
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        // Hours, Minutes, Seconds from time part
        3 | 4 | 5 => {
            let time_str = match time_part {
                Some(t) => t,
                None => return Value::Null,
            };
            // Strip timezone suffix (Z, +HH:MM, -HH:MM)
            let time_core = time_str
                .trim_end_matches('Z')
                .split(&['+', '-'][..])
                .next()
                .unwrap_or(time_str);
            let time_fields: Vec<&str> = time_core.splitn(3, ':').collect();
            let idx = part_index - 3;
            time_fields
                .get(idx)
                .and_then(|p| {
                    // For seconds, handle fractional part
                    let numeric = p.split('.').next().unwrap_or(p);
                    numeric.parse::<i64>().ok()
                })
                .map(Value::Integer)
                .unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn fn_year(args: &[Value]) -> Value {
    extract_datetime_part(args, 0)
}

fn fn_month(args: &[Value]) -> Value {
    extract_datetime_part(args, 1)
}

fn fn_day(args: &[Value]) -> Value {
    extract_datetime_part(args, 2)
}

fn fn_hours(args: &[Value]) -> Value {
    extract_datetime_part(args, 3)
}

fn fn_minutes(args: &[Value]) -> Value {
    extract_datetime_part(args, 4)
}

fn fn_seconds(args: &[Value]) -> Value {
    extract_datetime_part(args, 5)
}

fn fn_now() -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Value::String(format_epoch_as_datetime(now.as_secs()))
}

fn format_epoch_as_datetime(epoch_secs: u64) -> String {
    // Simple UTC datetime formatting
    let secs = epoch_secs;
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Approximate date calculation from epoch days
    let mut y = 1970i64;
    let mut remaining_days = days as i64;
    loop {
        let days_in_year = if is_leap_year(y) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        y += 1;
    }
    let days_in_months = if is_leap_year(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 1u32;
    for &dim in &days_in_months {
        if remaining_days < dim {
            break;
        }
        remaining_days -= dim;
        m += 1;
    }
    let d = remaining_days + 1;

    format!(
        "{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z"
    )
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

// ── Type testing functions ──────────────────────────────────────────────────

fn fn_isiri(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => Value::Boolean(v.is_iri()),
        None => Value::Null,
    }
}

fn fn_isblank(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => Value::Boolean(v.is_blank_node()),
        None => Value::Null,
    }
}

fn fn_isliteral(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => Value::Boolean(v.is_literal()),
        None => Value::Null,
    }
}

fn fn_isnumeric(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => Value::Boolean(v.is_numeric()),
        None => Value::Null,
    }
}

fn fn_bound(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => Value::Boolean(!v.is_null()),
        None => Value::Boolean(false),
    }
}

fn fn_datatype(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Integer(_)) => {
            Value::String("http://www.w3.org/2001/XMLSchema#integer".to_string())
        }
        Some(Value::Float(_)) => {
            Value::String("http://www.w3.org/2001/XMLSchema#double".to_string())
        }
        Some(Value::Boolean(_)) => {
            Value::String("http://www.w3.org/2001/XMLSchema#boolean".to_string())
        }
        Some(Value::String(_)) => {
            Value::String("http://www.w3.org/2001/XMLSchema#string".to_string())
        }
        Some(Value::Term(Term::Literal(lit))) => match lit.datatype() {
            Some(dt) => Value::String(dt.to_string()),
            None => Value::String("http://www.w3.org/2001/XMLSchema#string".to_string()),
        },
        _ => Value::Null,
    }
}

fn fn_lang(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Term(Term::Literal(lit))) => match lit.language() {
            Some(lang) => Value::String(lang.to_string()),
            None => Value::String(String::new()),
        },
        Some(Value::String(_)) => Value::String(String::new()),
        _ => Value::Null,
    }
}

// ── Constructor functions ───────────────────────────────────────────────────

fn fn_iri(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => {
            let iri = s.trim_start_matches('<').trim_end_matches('>');
            Value::Term(Term::Iri(IriTerm::new(iri)))
        }
        None => Value::Null,
    }
}

fn fn_bnode(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => Value::Term(Term::BlankNode(BlankNodeTerm::new(s))),
        None => {
            // Generate a blank node with a unique-ish id
            let id = format!("b{}", pseudo_random_u64());
            Value::Term(Term::BlankNode(BlankNodeTerm::new(id)))
        }
    }
}

fn fn_uuid() -> Value {
    // Simple UUID v4 approximation
    let r1 = pseudo_random_u64();
    let r2 = pseudo_random_u64();
    Value::String(format!(
        "urn:uuid:{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (r1 >> 32) as u32,
        (r1 >> 16) as u16,
        (r1 & 0xFFF) as u16,
        ((r2 >> 48) as u16 & 0x3FFF) | 0x8000,
        r2 & 0xFFFFFFFFFFFF,
    ))
}

fn fn_struuid() -> Value {
    let r1 = pseudo_random_u64();
    let r2 = pseudo_random_u64();
    Value::String(format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (r1 >> 32) as u32,
        (r1 >> 16) as u16,
        (r1 & 0xFFF) as u16,
        ((r2 >> 48) as u16 & 0x3FFF) | 0x8000,
        r2 & 0xFFFFFFFFFFFF,
    ))
}

// ── Casting functions ───────────────────────────────────────────────────────

fn fn_to_integer(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => match v.as_integer() {
            Some(i) => Value::Integer(i),
            None => match v.as_string().and_then(|s| s.parse::<i64>().ok()) {
                Some(i) => Value::Integer(i),
                None => Value::Null,
            },
        },
        None => Value::Null,
    }
}

fn fn_to_float(args: &[Value]) -> Value {
    match args.first() {
        Some(v) => match v.as_numeric() {
            Some(f) => Value::Float(f),
            None => match v.as_string().and_then(|s| s.parse::<f64>().ok()) {
                Some(f) => Value::Float(f),
                None => Value::Null,
            },
        },
        None => Value::Null,
    }
}

fn fn_to_boolean(args: &[Value]) -> Value {
    match args.first() {
        Some(Value::Boolean(b)) => Value::Boolean(*b),
        Some(Value::Integer(i)) => Value::Boolean(*i != 0),
        Some(Value::Float(f)) => Value::Boolean(*f != 0.0 && !f.is_nan()),
        Some(v) => match v.as_string() {
            Some(s) => match s.to_lowercase().as_str() {
                "true" | "1" => Value::Boolean(true),
                "false" | "0" => Value::Boolean(false),
                _ => Value::Null,
            },
            None => Value::Null,
        },
        None => Value::Null,
    }
}

// ── Hash functions ──────────────────────────────────────────────────────────

use sha2::Digest;

fn fn_hash<D: Digest>(args: &[Value]) -> Value {
    match args.first().and_then(|v| v.as_string()) {
        Some(s) => {
            let hash = D::digest(s.as_bytes());
            Value::String(bytes_to_hex(hash.as_slice()))
        }
        None => Value::Null,
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(hex, "{:02x}", byte).unwrap();
    }
    hex
}

// ── Utility ─────────────────────────────────────────────────────────────────

/// Converts a Value to a boolean for use in conditions.
pub fn value_to_bool(value: &Value) -> bool {
    match value {
        Value::Boolean(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0 && !f.is_nan(),
        Value::String(s) => !s.is_empty(),
        Value::Term(_) => true,
        Value::Null => false,
        _ => false,
    }
}

/// Simple pseudo-random number generation using system time + atomic counter.
/// Not cryptographically secure — used for SPARQL `rand()`, UUID, bnode generation.
fn pseudo_random() -> f64 {
    let mixed = next_pseudo_random_seed();
    (mixed as f64) / (u64::MAX as f64)
}

fn pseudo_random_u64() -> u64 {
    next_pseudo_random_seed()
}

/// Returns a unique pseudo-random seed by combining system time with an atomic counter.
/// This avoids collisions when called multiple times within the same nanosecond.
fn next_pseudo_random_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = time.wrapping_add(count as u128);
    seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs() {
        assert_eq!(call_builtin("abs", &[Value::Integer(-5)]), Value::Integer(5));
        assert_eq!(call_builtin("abs", &[Value::Float(-3.14)]), Value::Float(3.14));
        assert_eq!(call_builtin("ABS", &[Value::Null]), Value::Null);
    }

    #[test]
    fn test_ceil_floor_round() {
        assert_eq!(call_builtin("ceil", &[Value::Float(3.2)]), Value::Float(4.0));
        assert_eq!(call_builtin("floor", &[Value::Float(3.8)]), Value::Float(3.0));
        assert_eq!(call_builtin("round", &[Value::Float(3.5)]), Value::Float(4.0));
    }

    #[test]
    fn test_rand() {
        let r = call_builtin("rand", &[]);
        assert!(matches!(r, Value::Float(_)));
    }

    #[test]
    fn test_concat() {
        let result = call_builtin(
            "concat",
            &[Value::String("hello".into()), Value::String(" world".into())],
        );
        assert_eq!(result, Value::String("hello world".into()));
    }

    #[test]
    fn test_strlen() {
        assert_eq!(
            call_builtin("strlen", &[Value::String("hello".into())]),
            Value::Integer(5)
        );
        // Multi-byte: char count, not byte count
        assert_eq!(
            call_builtin("strlen", &[Value::String("café".into())]),
            Value::Integer(4)
        );
    }

    #[test]
    fn test_substr() {
        assert_eq!(
            call_builtin(
                "substr",
                &[Value::String("hello".into()), Value::Integer(2), Value::Integer(3)]
            ),
            Value::String("ell".into())
        );
    }

    #[test]
    fn test_ucase_lcase() {
        assert_eq!(
            call_builtin("ucase", &[Value::String("hello".into())]),
            Value::String("HELLO".into())
        );
        assert_eq!(
            call_builtin("lcase", &[Value::String("HELLO".into())]),
            Value::String("hello".into())
        );
    }

    #[test]
    fn test_contains_strstarts_strends() {
        assert_eq!(
            call_builtin(
                "contains",
                &[Value::String("hello world".into()), Value::String("world".into())]
            ),
            Value::Boolean(true)
        );
        assert_eq!(
            call_builtin(
                "strstarts",
                &[Value::String("hello".into()), Value::String("hel".into())]
            ),
            Value::Boolean(true)
        );
        assert_eq!(
            call_builtin(
                "strends",
                &[Value::String("hello".into()), Value::String("llo".into())]
            ),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_str() {
        assert_eq!(
            call_builtin("str", &[Value::Integer(42)]),
            Value::String("42".into())
        );
    }

    #[test]
    fn test_isiri() {
        let iri = Value::Term(Term::Iri(IriTerm::new("http://example.org")));
        assert_eq!(call_builtin("isIRI", &[iri]), Value::Boolean(true));
        assert_eq!(
            call_builtin("isIRI", &[Value::Integer(1)]),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_isnumeric() {
        assert_eq!(
            call_builtin("isnumeric", &[Value::Integer(1)]),
            Value::Boolean(true)
        );
        assert_eq!(
            call_builtin("isnumeric", &[Value::String("abc".into())]),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_bound() {
        assert_eq!(call_builtin("bound", &[Value::Integer(1)]), Value::Boolean(true));
        assert_eq!(call_builtin("bound", &[Value::Null]), Value::Boolean(false));
    }

    #[test]
    fn test_datatype() {
        assert_eq!(
            call_builtin("datatype", &[Value::Integer(42)]),
            Value::String("http://www.w3.org/2001/XMLSchema#integer".into())
        );
    }

    #[test]
    fn test_iri_constructor() {
        let result = call_builtin("iri", &[Value::String("http://example.org".into())]);
        assert!(matches!(result, Value::Term(Term::Iri(_))));
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(
            call_builtin("coalesce", &[Value::Null, Value::Integer(42)]),
            Value::Integer(42)
        );
        assert_eq!(call_builtin("coalesce", &[Value::Null, Value::Null]), Value::Null);
    }

    #[test]
    fn test_now() {
        let result = call_builtin("now", &[]);
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_uuid() {
        let result = fn_uuid();
        if let Value::String(s) = result {
            assert!(s.starts_with("urn:uuid:"));
        } else {
            panic!("expected string");
        }
    }

    #[test]
    fn test_struuid() {
        let result = fn_struuid();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_value_to_bool() {
        assert!(value_to_bool(&Value::Boolean(true)));
        assert!(!value_to_bool(&Value::Boolean(false)));
        assert!(value_to_bool(&Value::Integer(1)));
        assert!(!value_to_bool(&Value::Integer(0)));
        assert!(!value_to_bool(&Value::Null));
        assert!(value_to_bool(&Value::String("x".into())));
        assert!(!value_to_bool(&Value::String(String::new())));
    }

    #[test]
    fn test_null_propagation() {
        assert_eq!(call_builtin("abs", &[Value::Null]), Value::Null);
        assert_eq!(call_builtin("strlen", &[Value::Null]), Value::Null);
        assert_eq!(
            call_builtin("contains", &[Value::Null, Value::String("x".into())]),
            Value::Null
        );
    }

    #[test]
    fn test_unknown_function() {
        assert_eq!(call_builtin("nonexistent", &[Value::Integer(1)]), Value::Null);
    }

    #[test]
    fn test_md5_known_value() {
        assert_eq!(
            call_builtin("md5", &[Value::String("hello".into())]),
            Value::String("5d41402abc4b2a76b9719d911017c592".into())
        );
    }

    #[test]
    fn test_sha1_known_value() {
        assert_eq!(
            call_builtin("sha1", &[Value::String("hello".into())]),
            Value::String("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".into())
        );
    }

    #[test]
    fn test_sha256_known_value() {
        assert_eq!(
            call_builtin("sha256", &[Value::String("hello".into())]),
            Value::String("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into())
        );
    }

    #[test]
    fn test_sha384_known_value() {
        assert_eq!(
            call_builtin("sha384", &[Value::String("hello".into())]),
            Value::String("59e1748777448c69de6b800d7a33bbfb9ff1b463e44354c3553bcdb9c666fa90125a3c79f90397bdf5f6a13de828684f".into())
        );
    }

    #[test]
    fn test_sha512_known_value() {
        assert_eq!(
            call_builtin("sha512", &[Value::String("hello".into())]),
            Value::String("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043".into())
        );
    }

    #[test]
    fn test_hash_null_input() {
        assert_eq!(call_builtin("md5", &[Value::Null]), Value::Null);
        assert_eq!(call_builtin("sha256", &[Value::Null]), Value::Null);
    }

    #[test]
    fn test_hash_empty_string() {
        // MD5 of "" = d41d8cd98f00b204e9800998ecf8427e
        assert_eq!(
            call_builtin("md5", &[Value::String(String::new())]),
            Value::String("d41d8cd98f00b204e9800998ecf8427e".into())
        );
    }

    #[test]
    fn test_to_integer() {
        assert_eq!(
            call_builtin("integer", &[Value::Float(3.7)]),
            Value::Integer(3)
        );
        assert_eq!(
            call_builtin("integer", &[Value::String("42".into())]),
            Value::Integer(42)
        );
    }

    #[test]
    fn test_to_float() {
        assert_eq!(
            call_builtin("float", &[Value::Integer(42)]),
            Value::Float(42.0)
        );
    }

    #[test]
    fn test_to_boolean() {
        assert_eq!(
            call_builtin("boolean", &[Value::String("true".into())]),
            Value::Boolean(true)
        );
        assert_eq!(
            call_builtin("boolean", &[Value::String("false".into())]),
            Value::Boolean(false)
        );
        // Numeric inputs
        assert_eq!(
            call_builtin("boolean", &[Value::Integer(1)]),
            Value::Boolean(true)
        );
        assert_eq!(
            call_builtin("boolean", &[Value::Integer(0)]),
            Value::Boolean(false)
        );
        assert_eq!(
            call_builtin("boolean", &[Value::Float(0.0)]),
            Value::Boolean(false)
        );
    }
}
