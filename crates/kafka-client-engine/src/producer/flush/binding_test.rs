//! Exact flush and completion-generation binding scenarios.

use kafka_client_core::FlushId;

use crate::completion::CompletionId;

use super::{FlushBindingError, FlushBindings};

#[test]
fn binding_is_bidirectional_and_removed_only_by_exact_generation() {
    let mut bindings = FlushBindings::new(1);
    let flush_id = FlushId::from_raw(7);
    let completion_id = CompletionId::from_parts_for_test(0, 2);

    assert_eq!(bindings.bind(flush_id, completion_id), Ok(()));
    assert_eq!(bindings.completion(flush_id), Some(completion_id));
    assert_eq!(bindings.flush(completion_id), Some(flush_id));
    assert_eq!(
        bindings.remove_exact(flush_id, CompletionId::from_parts_for_test(0, 1)),
        Err(FlushBindingError::CompletionMismatch)
    );
    assert_eq!(bindings.remove_exact(flush_id, completion_id), Ok(()));
    assert_eq!(bindings.len(), 0);
}

#[test]
fn capacity_and_duplicate_ownership_are_rejected() {
    let mut bindings = FlushBindings::new(1);
    let first = CompletionId::from_parts_for_test(0, 1);
    assert_eq!(bindings.bind(FlushId::from_raw(1), first), Ok(()));
    assert_eq!(
        bindings.bind(
            FlushId::from_raw(1),
            CompletionId::from_parts_for_test(1, 1)
        ),
        Err(FlushBindingError::DuplicateFlush)
    );
    assert_eq!(
        bindings.bind(FlushId::from_raw(2), first),
        Err(FlushBindingError::DuplicateCompletion)
    );
    assert_eq!(
        bindings.bind(
            FlushId::from_raw(2),
            CompletionId::from_parts_for_test(1, 1)
        ),
        Err(FlushBindingError::Full)
    );
}
