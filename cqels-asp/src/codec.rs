//! Bidirectional encoding between RDF terms and ASP terms.
//!
//! [`AspTermCodec`] provides methods to encode [`cqels_model::Term`] values
//! into ASP-friendly string representations and decode them back.

use cqels_model::{BlankNodeTerm, IriTerm, LiteralTerm, Term};

/// Codec for bidirectional conversion between RDF terms and ASP term strings.
///
/// # Encoding scheme
///
/// | RDF term               | ASP encoding                           |
/// |------------------------|----------------------------------------|
/// | IRI                    | `iri("http://...")`                    |
/// | Typed literal          | `literal_dt("value","datatype")`       |
/// | Language-tagged literal| `literal_lang("value","lang")`         |
/// | Plain literal          | `literal_dt("value","xsd:string")`     |
/// | Blank node             | `bnode("id")`                          |
///
/// # Examples
///
/// ```
/// use cqels_asp::AspTermCodec;
///
/// let encoded = AspTermCodec::encode_iri("http://example.org/s");
/// assert_eq!(encoded, r#"iri("http://example.org/s")"#);
/// ```
pub struct AspTermCodec;

/// The XSD string datatype IRI, used as default for plain literals.
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

impl AspTermCodec {
    /// Encodes an IRI as an ASP term.
    pub fn encode_iri(iri: &str) -> String {
        format!("iri(\"{iri}\")")
    }

    /// Encodes a typed literal as an ASP term.
    pub fn encode_literal(value: &str, datatype: &str) -> String {
        format!("literal_dt(\"{value}\",\"{datatype}\")")
    }

    /// Encodes a language-tagged literal as an ASP term.
    pub fn encode_literal_lang(value: &str, lang: &str) -> String {
        format!("literal_lang(\"{value}\",\"{lang}\")")
    }

    /// Encodes a blank node as an ASP term.
    pub fn encode_blank(id: &str) -> String {
        format!("bnode(\"{id}\")")
    }

    /// Encodes an RDF [`Term`] into its ASP string representation.
    pub fn encode_term(term: &Term) -> String {
        match term {
            Term::Iri(iri) => Self::encode_iri(iri.as_str()),
            Term::BlankNode(bn) => Self::encode_blank(bn.id()),
            Term::Literal(lit) => {
                if let Some(lang) = lit.language() {
                    Self::encode_literal_lang(lit.value(), lang)
                } else if let Some(dt) = lit.datatype() {
                    Self::encode_literal(lit.value(), dt)
                } else {
                    Self::encode_literal(lit.value(), XSD_STRING)
                }
            }
            _ => Self::encode_literal(&format!("{term:?}"), XSD_STRING),
        }
    }

    /// Decodes an ASP term string back into an RDF [`Term`].
    ///
    /// Returns `None` if the string does not match any known encoding pattern.
    pub fn decode_term(asp_term: &str) -> Option<Term> {
        let trimmed = asp_term.trim();

        if let Some(inner) = strip_functor(trimmed, "iri") {
            let iri = strip_quotes(inner)?;
            Some(Term::Iri(IriTerm::new(iri)))
        } else if let Some(inner) = strip_functor(trimmed, "bnode") {
            let id = strip_quotes(inner)?;
            Some(Term::BlankNode(BlankNodeTerm::new(id)))
        } else if let Some(inner) = strip_functor(trimmed, "literal_lang") {
            let (value, lang) = split_two_quoted_args(inner)?;
            Some(Term::Literal(LiteralTerm::new(value).with_language(lang)))
        } else if let Some(inner) = strip_functor(trimmed, "literal_dt") {
            let (value, datatype) = split_two_quoted_args(inner)?;
            if datatype == XSD_STRING {
                Some(Term::Literal(LiteralTerm::new(value)))
            } else {
                Some(Term::Literal(
                    LiteralTerm::new(value).with_datatype(datatype),
                ))
            }
        } else {
            None
        }
    }
}

/// Strips a functor name and outer parentheses, e.g., `iri("http://...")` -> `"http://..."`.
fn strip_functor<'a>(s: &'a str, functor: &str) -> Option<&'a str> {
    let s = s.strip_prefix(functor)?;
    let s = s.strip_prefix('(')?;
    let s = s.strip_suffix(')')?;
    Some(s)
}

/// Strips surrounding double quotes from a string.
fn strip_quotes(s: &str) -> Option<&str> {
    let s = s.trim();
    let s = s.strip_prefix('"')?;
    let s = s.strip_suffix('"')?;
    Some(s)
}

/// Splits a string like `"value","other"` into two unquoted strings.
fn split_two_quoted_args(s: &str) -> Option<(&str, &str)> {
    // Expected pattern: "value1","value2"
    // Find the comma that separates the two quoted strings.
    // We look for "," as the separator.
    let separator = "\",\"";
    let pos = s.find(separator)?;
    let first = s[..pos].strip_prefix('"')?;
    let second = s[pos + separator.len()..].strip_suffix('"')?;
    Some((first, second))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_iri() {
        assert_eq!(
            AspTermCodec::encode_iri("http://example.org/s"),
            r#"iri("http://example.org/s")"#
        );
    }

    #[test]
    fn test_encode_literal() {
        assert_eq!(
            AspTermCodec::encode_literal("42", "http://www.w3.org/2001/XMLSchema#integer"),
            r#"literal_dt("42","http://www.w3.org/2001/XMLSchema#integer")"#
        );
    }

    #[test]
    fn test_encode_literal_lang() {
        assert_eq!(
            AspTermCodec::encode_literal_lang("hello", "en"),
            r#"literal_lang("hello","en")"#
        );
    }

    #[test]
    fn test_encode_blank() {
        assert_eq!(AspTermCodec::encode_blank("b0"), r#"bnode("b0")"#);
    }

    #[test]
    fn test_encode_term_iri() {
        let term = Term::Iri(IriTerm::new("http://example.org/resource"));
        let encoded = AspTermCodec::encode_term(&term);
        assert_eq!(encoded, r#"iri("http://example.org/resource")"#);
    }

    #[test]
    fn test_encode_term_plain_literal() {
        let term = Term::Literal(LiteralTerm::new("hello"));
        let encoded = AspTermCodec::encode_term(&term);
        assert_eq!(
            encoded,
            r#"literal_dt("hello","http://www.w3.org/2001/XMLSchema#string")"#
        );
    }

    #[test]
    fn test_decode_iri() {
        let decoded = AspTermCodec::decode_term(r#"iri("http://example.org/s")"#).unwrap();
        assert_eq!(decoded, Term::Iri(IriTerm::new("http://example.org/s")));
    }

    #[test]
    fn test_decode_blank() {
        let decoded = AspTermCodec::decode_term(r#"bnode("b42")"#).unwrap();
        assert_eq!(decoded, Term::BlankNode(BlankNodeTerm::new("b42")));
    }

    #[test]
    fn test_decode_literal_lang() {
        let decoded = AspTermCodec::decode_term(r#"literal_lang("bonjour","fr")"#).unwrap();
        let lit = decoded.as_literal().unwrap();
        assert_eq!(lit.value(), "bonjour");
        assert_eq!(lit.language(), Some("fr"));
    }

    #[test]
    fn test_decode_typed_literal() {
        let decoded = AspTermCodec::decode_term(
            r#"literal_dt("42","http://www.w3.org/2001/XMLSchema#integer")"#,
        )
        .unwrap();
        let lit = decoded.as_literal().unwrap();
        assert_eq!(lit.value(), "42");
        assert_eq!(
            lit.datatype(),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
    }

    #[test]
    fn test_decode_plain_literal_roundtrip() {
        // Plain literal encodes with xsd:string, decodes back to plain
        let term = Term::Literal(LiteralTerm::new("hello"));
        let encoded = AspTermCodec::encode_term(&term);
        let decoded = AspTermCodec::decode_term(&encoded).unwrap();
        assert_eq!(decoded, term);
    }

    #[test]
    fn test_decode_unknown_returns_none() {
        assert!(AspTermCodec::decode_term("unknown(x)").is_none());
        assert!(AspTermCodec::decode_term("just_a_string").is_none());
    }

    #[test]
    fn test_roundtrip_iri() {
        let term = Term::Iri(IriTerm::new("http://example.org/test#frag"));
        let encoded = AspTermCodec::encode_term(&term);
        let decoded = AspTermCodec::decode_term(&encoded).unwrap();
        assert_eq!(term, decoded);
    }

    #[test]
    fn test_roundtrip_blank_node() {
        let term = Term::BlankNode(BlankNodeTerm::new("node99"));
        let encoded = AspTermCodec::encode_term(&term);
        let decoded = AspTermCodec::decode_term(&encoded).unwrap();
        assert_eq!(term, decoded);
    }

    #[test]
    fn test_roundtrip_language_literal() {
        let term = Term::Literal(LiteralTerm::new("hola").with_language("es"));
        let encoded = AspTermCodec::encode_term(&term);
        let decoded = AspTermCodec::decode_term(&encoded).unwrap();
        assert_eq!(term, decoded);
    }
}
