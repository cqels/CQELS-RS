# Data Model

> **Prerequisites**: [Getting Started](getting-started.md)
>
> **Next steps**: [Stream Processing](stream-processing.md), [Architecture](architecture.md)

The `cqels-model` crate provides the foundational RDF types used throughout
the engine. All other crates depend on it.

## Terms

A `Term` is the building block of RDF data. It comes in three variants:

```rust
use cqels_model::Term;
use cqels_model::term::{IriTerm, BlankNodeTerm, LiteralTerm};

// IRI -- a globally unique identifier
let iri = Term::Iri(IriTerm::new("http://example.org/sensor/1"));
assert!(iri.is_iri());
println!("{}", iri); // <http://example.org/sensor/1>

// Blank node -- a locally scoped anonymous identifier
let blank = Term::BlankNode(BlankNodeTerm::new("b0"));
assert!(blank.is_blank_node());
println!("{}", blank); // _:b0

// Literal -- a typed or language-tagged string value
let lit = Term::Literal(LiteralTerm::new("42.0"));
assert!(lit.is_literal());
println!("{}", lit); // "42.0"
```

### IriTerm

An Internationalized Resource Identifier:

```rust
let iri = IriTerm::new("http://example.org/temperature");
assert_eq!(iri.as_str(), "http://example.org/temperature");
```

Display format wraps the IRI in angle brackets: `<http://example.org/temperature>`.

### BlankNodeTerm

An anonymous node with a locally unique identifier:

```rust
let bnode = BlankNodeTerm::new("node42");
assert_eq!(bnode.id(), "node42");
```

### LiteralTerm

A string value, optionally with a datatype IRI or language tag:

```rust
// Plain literal
let plain = LiteralTerm::new("hello");

// Typed literal (XSD double)
let typed = LiteralTerm::new("42.0")
    .with_datatype("http://www.w3.org/2001/XMLSchema#double");
assert_eq!(typed.value(), "42.0");
assert_eq!(typed.datatype(), Some("http://www.w3.org/2001/XMLSchema#double"));

// Language-tagged literal
let tagged = LiteralTerm::new("Bonjour")
    .with_language("fr");
assert_eq!(tagged.language(), Some("fr"));
```

### Type Inspection

Use accessor methods to inspect the variant:

| Method | Returns |
|--------|---------|
| `is_iri()` | `bool` |
| `is_blank_node()` | `bool` |
| `is_literal()` | `bool` |
| `as_iri()` | `Option<&IriTerm>` |
| `as_blank_node()` | `Option<&BlankNodeTerm>` |
| `as_literal()` | `Option<&LiteralTerm>` |

## Statements

A `Statement` is an RDF triple (subject, predicate, object) with an optional
named graph, making it a quad:

```rust
use cqels_model::{Term, Statement};
use cqels_model::term::{IriTerm, LiteralTerm};

// Triple
let stmt = Statement::new(
    Term::Iri(IriTerm::new("http://sensor/1")),
    IriTerm::new("http://example.org/temperature"),
    Term::Literal(LiteralTerm::new("42.0")),
);
assert!(!stmt.is_quad());

// Quad (triple + named graph)
let quad = Statement::new_quad(
    Term::Iri(IriTerm::new("http://sensor/1")),
    IriTerm::new("http://example.org/temperature"),
    Term::Literal(LiteralTerm::new("42.0")),
    IriTerm::new("http://graph/sensors"),
);
assert!(quad.is_quad());
```

Fields are public: `subject: Term`, `predicate: IriTerm`, `object: Term`,
`graph: Option<IriTerm>`. Note that `predicate` is always an `IriTerm`
(not a `Term`), matching the RDF specification.

## Values

`Value` is a dynamically typed runtime value used during query evaluation:

```rust
use cqels_model::Value;
use cqels_model::term::IriTerm;
use cqels_model::Term;

// Create from primitives
let s = Value::from("hello");
let i = Value::from(42i64);
let f = Value::from(3.14f64);
let b = Value::from(true);
let n = Value::Null;

// Access with type-safe getters
assert_eq!(s.as_string(), Some("hello"));
assert_eq!(i.as_integer(), Some(42));
assert_eq!(f.as_float(), Some(3.14));
assert_eq!(b.as_boolean(), Some(true));
assert!(n.is_null());

// Convert from/to Term
let term = Term::Iri(IriTerm::new("http://example.org/x"));
let v = Value::from_term(term.clone());
assert!(v.is_iri());
assert_eq!(v.to_term(), Some(term));
```

`From` conversions exist for `i64`, `f64`, `bool`, `String`, `&str`,
`Term`, and `IriTerm`.

## BindingSet

A `BindingSet` is a set of variable-to-value mappings produced by query
pattern matching, carrying a timestamp:

```rust
use cqels_model::{BindingSet, Value};

// Create and populate
let mut bs = BindingSet::new(1000); // timestamp = 1000ms
bs.insert("sensor", Value::from("http://sensor/1"));
bs.insert("temp", Value::from(42.0));

// Access
assert_eq!(bs.get("sensor"), Some(&Value::from("http://sensor/1")));
assert!(bs.contains("temp"));
assert_eq!(bs.len(), 2);
assert_eq!(bs.timestamp(), 1000);

// Iterate
for (var, val) in bs.iter() {
    println!("{var} = {val}");
}
```

### Join Semantics

Two binding sets are **compatible** if they agree on all shared variables.
A `join` merges compatible sets into one:

```rust
let mut a = BindingSet::new(0);
a.insert("x", Value::from(1i64));
a.insert("y", Value::from(2i64));

let mut b = BindingSet::new(0);
b.insert("x", Value::from(1i64)); // same value for x
b.insert("z", Value::from(3i64));

assert!(a.is_compatible(&b));

let joined = a.join(&b).unwrap();
assert_eq!(joined.len(), 3); // x, y, z
```

If the shared variable `x` had different values, `join` returns `None`.

### Required Access

Use `get_required` to return an error instead of `None`:

```rust
let result = bs.get_required("missing");
assert!(result.is_err()); // CqelsError::BindingNotFound { variable: "missing" }
```

## Error Types

All errors funnel through `CqelsError`:

| Variant | Description |
|---------|-------------|
| `Parse(ParseErrorDetail)` | Query parse error with kind, message, line, column |
| `Evaluation { message }` | Runtime evaluation error |
| `Stream { message }` | Stream processing error |
| `Window { message }` | Window operation error |
| `Join { message }` | Join operation error |
| `Reasoning { message }` | Inference engine error |
| `UnsupportedOperation { operation }` | Unsupported feature |
| `InvalidTerm { detail }` | Invalid RDF term construction |
| `BindingNotFound { variable }` | Variable not in binding set |
| `Io(std::io::Error)` | I/O error |

### ParseErrorDetail

Parse errors include structured location information:

```rust
use cqels_model::{ParseErrorDetail, ParseErrorKind};

let err = ParseErrorDetail::syntax("unexpected token")
    .with_location(5, 12);

assert_eq!(err.kind, ParseErrorKind::Syntax);
assert_eq!(err.line, Some(5));
assert_eq!(err.column, Some(12));
```

Kind variants: `Syntax`, `Semantic`, `Unsupported`.

### CqelsResult

The convenience alias `CqelsResult<T>` is `Result<T, CqelsError>`.

## Oxrdf Interoperability

Bidirectional conversions with [`oxrdf`](https://docs.rs/oxrdf) types:

```rust
use oxrdf::{NamedNode, Literal, Quad};
use cqels_model::{Term, Statement};
use cqels_model::term::IriTerm;

// oxrdf -> cqels-model
let nn = NamedNode::new("http://example.org/x").unwrap();
let iri = IriTerm::from(nn);

let ox_term = oxrdf::Term::NamedNode(NamedNode::new("http://example.org/y").unwrap());
let term = Term::from(ox_term);

// cqels-model -> oxrdf (TryFrom, may fail for invalid IRIs)
let ox: Result<oxrdf::Term, _> = (&term).try_into();
```

`Statement` converts to/from `oxrdf::Quad`. The `TryFrom` direction can fail
if the term contains an invalid IRI string.
