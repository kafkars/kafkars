//! Producer completion reclaim diagnostic scenarios.

use crate::completion::{CompletionId, CompletionRegistryError};

use super::CompletionReclaimError;

#[test]
fn missing_and_ambiguous_bindings_are_distinct() {
    let id = CompletionId::from_parts_for_test(0, 1);

    assert_ne!(
        CompletionReclaimError::UnknownBinding(id),
        CompletionReclaimError::AmbiguousBinding(id)
    );
    assert!(
        CompletionReclaimError::AmbiguousBinding(id)
            .to_string()
            .contains("multiple")
    );
}

#[test]
fn registry_failure_retains_its_source_category() {
    let error = CompletionReclaimError::from(CompletionRegistryError::Full);
    assert_eq!(
        error,
        CompletionReclaimError::Registry(CompletionRegistryError::Full)
    );
}
