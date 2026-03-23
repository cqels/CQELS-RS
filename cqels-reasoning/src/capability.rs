//! Reasoning capability flags and profile→capability mappings.
//!
//! Each [`ReasoningCapability`] represents a specific inference ability.
//! The [`capabilities_for`] function maps a [`ReasoningProfile`] to the
//! set of capabilities it supports.

use std::collections::HashSet;

use crate::profile::ReasoningProfile;

/// Individual reasoning capabilities that a profile may support.
///
/// # Examples
///
/// ```
/// use cqels_reasoning::{ReasoningProfile, ReasoningCapability, capabilities_for, supports};
///
/// let caps = capabilities_for(ReasoningProfile::Rdfs);
/// assert!(caps.contains(&ReasoningCapability::RdfsSubpropertyPropagation));
/// assert!(!caps.contains(&ReasoningCapability::OwlTransitivePropertyPropagation));
///
/// assert!(supports(ReasoningProfile::OwlLite, ReasoningCapability::OwlTransitivePropertyPropagation));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ReasoningCapability {
    /// rdfs7 — subproperty propagation.
    RdfsSubpropertyPropagation,
    /// rdfs2/rdfs3 — domain and range inference.
    RdfsDomainRangeInference,
    /// rdfs9 — subclass type propagation.
    RdfsSubclassTypePropagation,
    /// rdfs1, rdfs4, rdfs8, rdfs10, rdfs12, rdfs13 — non-axiomatic families.
    RdfsFullNonAxiomaticFamilies,
    /// owl:equivalentClass / owl:equivalentProperty closure.
    OwlEquivalentClassPropertyClosure,
    /// owl:inverseOf propagation.
    OwlInversePropertyPropagation,
    /// owl:SymmetricProperty propagation.
    OwlSymmetricPropertyPropagation,
    /// owl:ReflexiveProperty propagation (OWL 2 RL only).
    OwlReflexivePropertyPropagation,
    /// owl:TransitiveProperty propagation.
    OwlTransitivePropertyPropagation,
    /// owl:hasValue restriction propagation (OWL 2 RL only).
    OwlHasValueRestrictionPropagation,
    /// owl:sameAs symmetry and transitivity.
    OwlSameAsSymmetry,
    /// OWL 2 EL taxonomy closure.
    OwlElTaxonomyClosure,
    /// owl:someValuesFrom (OWL-QL).
    OwlSomeValuesFrom,
    /// owl:disjointWith detection (OWL-QL).
    OwlDisjointnessDetection,
}

/// Returns the set of capabilities supported by the given profile.
pub fn capabilities_for(profile: ReasoningProfile) -> HashSet<ReasoningCapability> {
    use ReasoningCapability::*;
    match profile {
        ReasoningProfile::None => HashSet::new(),
        ReasoningProfile::Rdfs => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
        ]
        .into_iter()
        .collect(),
        ReasoningProfile::RdfsFull => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
            RdfsFullNonAxiomaticFamilies,
        ]
        .into_iter()
        .collect(),
        ReasoningProfile::OwlLite => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
            OwlEquivalentClassPropertyClosure,
            OwlInversePropertyPropagation,
            OwlSymmetricPropertyPropagation,
            OwlTransitivePropertyPropagation,
            OwlSameAsSymmetry,
        ]
        .into_iter()
        .collect(),
        ReasoningProfile::OwlQl => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
            OwlEquivalentClassPropertyClosure,
            OwlInversePropertyPropagation,
            OwlSomeValuesFrom,
            OwlDisjointnessDetection,
        ]
        .into_iter()
        .collect(),
        ReasoningProfile::Owl2El => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
            RdfsFullNonAxiomaticFamilies,
            OwlEquivalentClassPropertyClosure,
            OwlElTaxonomyClosure,
        ]
        .into_iter()
        .collect(),
        ReasoningProfile::Owl2Rl => [
            RdfsSubpropertyPropagation,
            RdfsDomainRangeInference,
            RdfsSubclassTypePropagation,
            RdfsFullNonAxiomaticFamilies,
            OwlEquivalentClassPropertyClosure,
            OwlInversePropertyPropagation,
            OwlSymmetricPropertyPropagation,
            OwlReflexivePropertyPropagation,
            OwlTransitivePropertyPropagation,
            OwlHasValueRestrictionPropagation,
            OwlSameAsSymmetry,
        ]
        .into_iter()
        .collect(),
    }
}

/// Returns whether the given profile supports a specific capability.
pub fn supports(profile: ReasoningProfile, cap: ReasoningCapability) -> bool {
    capabilities_for(profile).contains(&cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdfs_capabilities() {
        let caps = capabilities_for(ReasoningProfile::Rdfs);
        assert_eq!(caps.len(), 3);
        assert!(caps.contains(&ReasoningCapability::RdfsSubpropertyPropagation));
        assert!(caps.contains(&ReasoningCapability::RdfsDomainRangeInference));
        assert!(caps.contains(&ReasoningCapability::RdfsSubclassTypePropagation));
    }

    #[test]
    fn test_rdfs_full_adds_non_axiomatic() {
        let caps = capabilities_for(ReasoningProfile::RdfsFull);
        assert_eq!(caps.len(), 4);
        assert!(caps.contains(&ReasoningCapability::RdfsFullNonAxiomaticFamilies));
    }

    #[test]
    fn test_owl_lite_capabilities() {
        let caps = capabilities_for(ReasoningProfile::OwlLite);
        assert_eq!(caps.len(), 8);
        assert!(caps.contains(&ReasoningCapability::OwlTransitivePropertyPropagation));
        assert!(caps.contains(&ReasoningCapability::OwlSameAsSymmetry));
    }

    #[test]
    fn test_owl_ql_capabilities() {
        let caps = capabilities_for(ReasoningProfile::OwlQl);
        assert_eq!(caps.len(), 7);
        assert!(caps.contains(&ReasoningCapability::OwlSomeValuesFrom));
        assert!(caps.contains(&ReasoningCapability::OwlDisjointnessDetection));
    }

    #[test]
    fn test_owl2_el_capabilities() {
        let caps = capabilities_for(ReasoningProfile::Owl2El);
        assert_eq!(caps.len(), 6);
        assert!(caps.contains(&ReasoningCapability::OwlElTaxonomyClosure));
        // OWL 2 EL should NOT have transitive/symmetric/inverse
        assert!(!caps.contains(&ReasoningCapability::OwlTransitivePropertyPropagation));
        assert!(!caps.contains(&ReasoningCapability::OwlSymmetricPropertyPropagation));
        assert!(!caps.contains(&ReasoningCapability::OwlInversePropertyPropagation));
    }

    #[test]
    fn test_owl2_rl_capabilities() {
        let caps = capabilities_for(ReasoningProfile::Owl2Rl);
        assert_eq!(caps.len(), 11);
        assert!(caps.contains(&ReasoningCapability::OwlReflexivePropertyPropagation));
        assert!(caps.contains(&ReasoningCapability::OwlHasValueRestrictionPropagation));
    }

    #[test]
    fn test_none_capabilities_empty() {
        assert!(capabilities_for(ReasoningProfile::None).is_empty());
    }

    #[test]
    fn test_supports_helper() {
        assert!(supports(
            ReasoningProfile::Rdfs,
            ReasoningCapability::RdfsSubpropertyPropagation
        ));
        assert!(!supports(
            ReasoningProfile::Rdfs,
            ReasoningCapability::OwlTransitivePropertyPropagation
        ));
    }
}
