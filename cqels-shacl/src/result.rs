//! SHACL validation and repair result types.
//!
//! These types represent the output of SHACL validation:
//! - [`ShaclValidationResult`] -- the top-level result
//! - [`ShaclViolation`] -- a single constraint violation
//! - [`ShaclRepairCandidate`] and [`ShaclRepairEdit`] -- repair suggestions

use cqels_model::Statement;

/// The overall status of a SHACL validation run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclValidationStatus {
    /// The data graph conforms to all shapes.
    Conforms,
    /// The data graph violates one or more shape constraints.
    Violates,
    /// The repair search was bounded (reached the model limit).
    SearchBounded,
    /// Repair candidates were found.
    RepairCandidatesFound,
}

/// The type of edit in a repair operation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclRepairOperation {
    /// Add a triple to the data graph.
    Add,
    /// Delete a triple from the data graph.
    Delete,
}

/// A single SHACL constraint violation.
#[derive(Clone, Debug)]
pub struct ShaclViolation {
    /// The shape that was violated.
    pub shape: String,
    /// The focus node that violated the shape.
    pub focus: String,
    /// The type of constraint violated (e.g. "minCount", "maxCount").
    pub constraint: String,
    /// Additional detail about the violation (e.g. the property path).
    pub detail: String,
}

/// A single edit operation in a repair candidate.
#[derive(Clone, Debug)]
pub struct ShaclRepairEdit {
    /// Whether this is an add or delete operation.
    pub operation: ShaclRepairOperation,
    /// The RDF statement to add or delete.
    pub statement: Statement,
}

/// A complete repair candidate consisting of one or more edits.
#[derive(Clone, Debug)]
pub struct ShaclRepairCandidate {
    /// The list of edits forming this repair.
    pub edits: Vec<ShaclRepairEdit>,
}

/// The result of a SHACL validation (and optional repair search) run.
#[derive(Clone, Debug)]
pub struct ShaclValidationResult {
    /// The timestamp of the window snapshot that was validated.
    pub timestamp: i64,
    /// The identifier of the window that produced the snapshot.
    pub window_id: String,
    /// Whether the data conforms to all shapes.
    pub conforms: bool,
    /// The list of violations found (empty if conforming).
    pub violations: Vec<ShaclViolation>,
    /// Repair candidates, if repair search was enabled and violations found.
    pub candidates: Vec<ShaclRepairCandidate>,
    /// The overall validation status.
    pub status: ShaclValidationStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqels_model::term::{IriTerm, LiteralTerm};
    use cqels_model::Term;

    #[test]
    fn test_validation_status_eq() {
        assert_eq!(
            ShaclValidationStatus::Conforms,
            ShaclValidationStatus::Conforms
        );
        assert_ne!(
            ShaclValidationStatus::Conforms,
            ShaclValidationStatus::Violates
        );
    }

    #[test]
    fn test_repair_operation_eq() {
        assert_eq!(ShaclRepairOperation::Add, ShaclRepairOperation::Add);
        assert_ne!(ShaclRepairOperation::Add, ShaclRepairOperation::Delete);
    }

    #[test]
    fn test_violation_fields() {
        let v = ShaclViolation {
            shape: "PersonShape".into(),
            focus: "http://example.org/alice".into(),
            constraint: "minCount".into(),
            detail: "http://example.org/name".into(),
        };
        assert_eq!(v.shape, "PersonShape");
        assert_eq!(v.constraint, "minCount");
    }

    #[test]
    fn test_repair_edit_add() {
        let stmt = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/alice")),
            IriTerm::new("http://example.org/name"),
            Term::Literal(LiteralTerm::new("Alice")),
        );
        let edit = ShaclRepairEdit {
            operation: ShaclRepairOperation::Add,
            statement: stmt,
        };
        assert_eq!(edit.operation, ShaclRepairOperation::Add);
    }

    #[test]
    fn test_validation_result_conforms() {
        let result = ShaclValidationResult {
            timestamp: 1000,
            window_id: "w1".into(),
            conforms: true,
            violations: vec![],
            candidates: vec![],
            status: ShaclValidationStatus::Conforms,
        };
        assert!(result.conforms);
        assert!(result.violations.is_empty());
        assert_eq!(result.status, ShaclValidationStatus::Conforms);
    }

    #[test]
    fn test_validation_result_with_violations() {
        let result = ShaclValidationResult {
            timestamp: 2000,
            window_id: "w2".into(),
            conforms: false,
            violations: vec![
                ShaclViolation {
                    shape: "S1".into(),
                    focus: "n1".into(),
                    constraint: "minCount".into(),
                    detail: "p1".into(),
                },
                ShaclViolation {
                    shape: "S1".into(),
                    focus: "n2".into(),
                    constraint: "maxCount".into(),
                    detail: "p2".into(),
                },
            ],
            candidates: vec![],
            status: ShaclValidationStatus::Violates,
        };
        assert!(!result.conforms);
        assert_eq!(result.violations.len(), 2);
    }

    #[test]
    fn test_repair_candidate_multiple_edits() {
        let stmt1 = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/a")),
            IriTerm::new("http://example.org/p"),
            Term::Literal(LiteralTerm::new("v1")),
        );
        let stmt2 = Statement::new(
            Term::Iri(IriTerm::new("http://example.org/b")),
            IriTerm::new("http://example.org/q"),
            Term::Literal(LiteralTerm::new("v2")),
        );
        let candidate = ShaclRepairCandidate {
            edits: vec![
                ShaclRepairEdit {
                    operation: ShaclRepairOperation::Add,
                    statement: stmt1,
                },
                ShaclRepairEdit {
                    operation: ShaclRepairOperation::Delete,
                    statement: stmt2,
                },
            ],
        };
        assert_eq!(candidate.edits.len(), 2);
        assert_eq!(candidate.edits[0].operation, ShaclRepairOperation::Add);
        assert_eq!(candidate.edits[1].operation, ShaclRepairOperation::Delete);
    }
}
