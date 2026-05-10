//! Query compilers that translate parsed ASTs into executable query pipelines.
//!
//! This module bridges the gap between parsed query definitions and operator execution:
//!
//! - [`pipeline`] — Shared pipeline utilities (pattern matching, filtering, projection)
//! - [`compiled`] — Compiled query types implementing `ContinuousQuery`
//! - [`cqelsql`] — CqelsQL (SPARQL-style) query compiler
//! - [`cypherql`] — CypherQL (Cypher-style) query compiler

pub mod compiled;
pub mod cqelsql;
pub mod cypherql;
pub mod pipeline;
pub mod self_join;

pub use compiled::{
    CompiledAskQuery, CompiledConstructQuery, CompiledCqelsQuery, CompiledCypherQuery,
    CompiledDescribeQuery,
};
pub use cqelsql::CqelsQueryCompiler;
pub use cypherql::CypherQueryCompiler;
pub use self_join::{detect_self_joins, self_join_hints_by_source, SelfJoinHint};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_re_exports_accessible() {
        // Verify that key types are re-exported and usable via the compiler module.
        // CqelsQueryCompiler and CypherQueryCompiler expose static compile() methods.
        fn _assert_cqels_compiler(_: &CqelsQueryCompiler) {}
        fn _assert_cypher_compiler(_: &CypherQueryCompiler) {}
        fn _assert_compiled_cqels(_: &CompiledCqelsQuery) {}
        fn _assert_compiled_cypher(_: &CompiledCypherQuery) {}
        fn _assert_compiled_ask(_: &CompiledAskQuery) {}
        fn _assert_compiled_describe(_: &CompiledDescribeQuery) {}
        fn _assert_compiled_construct(_: &CompiledConstructQuery) {}
    }
}
