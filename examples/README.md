# Examples

This is a small public examples project that consumes the released CQELS-RS
crates. It does not vendor or mirror the engine implementation.

Run the query-language example from the repository root:

```bash
cargo run --manifest-path examples/Cargo.toml
```

The example demonstrates parsing a CQELS-QL stream query. Additional engine,
reasoning, CEP, and storage examples can be added here as released APIs
stabilize.
