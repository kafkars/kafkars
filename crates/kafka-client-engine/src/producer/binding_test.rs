//! Scenarios for bounded operation and generation-fenced completion association.

use kafka_client_core::OperationId;

use crate::completion::{CompletionId, CompletionObserver, CompletionRegistry};

use super::{CompletionBindingError, CompletionBindings};

struct ReservedCompletion {
    id: CompletionId,
    observer: CompletionObserver<()>,
}

fn reserve(registry: &mut CompletionRegistry<()>) -> ReservedCompletion {
    let Ok((id, observer)) = registry.reserve() else {
        panic!("completion should reserve")
    };
    ReservedCompletion { id, observer }
}

fn rollback(registry: &mut CompletionRegistry<()>, reserved: ReservedCompletion) {
    assert_eq!(registry.rollback_reservation(reserved.id), Ok(()));
    drop(reserved.observer);
}

fn stop(mut registry: CompletionRegistry<()>) {
    let Ok(join) = registry.stop_notifier() else {
        panic!("settled notifier should stop")
    };
    assert_eq!(join.join_off_notifier(), Ok(()));
}

#[test]
fn capacity_and_duplicate_axes_are_rejected_independently() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start")
    };
    let first = reserve(&mut registry);
    let second = reserve(&mut registry);
    let excess = reserve(&mut registry);
    let mut bindings = CompletionBindings::new(2);

    assert_eq!(bindings.bind(OperationId::from_raw(1), first.id), Ok(()));
    assert_eq!(
        bindings.bind(OperationId::from_raw(1), second.id),
        Err(CompletionBindingError::DuplicateOperation)
    );
    assert_eq!(
        bindings.bind(OperationId::from_raw(2), first.id),
        Err(CompletionBindingError::DuplicateCompletion)
    );
    assert_eq!(bindings.bind(OperationId::from_raw(2), second.id), Ok(()));
    assert_eq!(
        bindings.bind(OperationId::from_raw(3), excess.id),
        Err(CompletionBindingError::Full)
    );

    rollback(&mut registry, first);
    rollback(&mut registry, second);
    rollback(&mut registry, excess);
    stop(registry);
}

#[test]
fn forward_reverse_removal_and_reuse_preserve_provenance() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start")
    };
    let first = reserve(&mut registry);
    let second = reserve(&mut registry);
    let unbound = reserve(&mut registry);
    let first_operation = OperationId::from_raw(7);
    let second_operation = OperationId::from_raw(9);
    let replacement = OperationId::from_raw(11);
    let mut bindings = CompletionBindings::new(2);

    assert_eq!(bindings.bind(first_operation, first.id), Ok(()));
    assert_eq!(bindings.bind(second_operation, second.id), Ok(()));
    assert_eq!(bindings.completion(first_operation), Some(first.id));
    assert_eq!(bindings.operation(second.id), Some(second_operation));
    assert_eq!(bindings.operation(unbound.id), None);
    assert_eq!(
        bindings.remove(OperationId::from_raw(8)),
        Err(CompletionBindingError::UnknownOperation)
    );
    assert_eq!(bindings.remove(first_operation), Ok(first.id));
    assert_eq!(bindings.completion(first_operation), None);
    assert_eq!(bindings.operation(first.id), None);
    assert_eq!(bindings.bind(replacement, first.id), Ok(()));

    rollback(&mut registry, first);
    rollback(&mut registry, second);
    rollback(&mut registry, unbound);
    stop(registry);
}

#[test]
fn reused_slot_generations_remain_distinct_binding_identities() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start")
    };
    let stale = reserve(&mut registry);
    let stale_id = stale.id;
    rollback(&mut registry, stale);
    let live = reserve(&mut registry);
    assert_ne!(stale_id, live.id);
    let mut bindings = CompletionBindings::new(2);

    assert_eq!(bindings.bind(OperationId::from_raw(1), stale_id), Ok(()));
    assert_eq!(bindings.bind(OperationId::from_raw(2), live.id), Ok(()));
    assert_eq!(bindings.operation(stale_id), Some(OperationId::from_raw(1)));
    assert_eq!(bindings.operation(live.id), Some(OperationId::from_raw(2)));

    rollback(&mut registry, live);
    stop(registry);
}

#[test]
fn exact_removal_rejects_a_different_completion_generation() {
    let Ok(mut registry) = CompletionRegistry::new(2, 2) else {
        panic!("notifier should start")
    };
    let owned = reserve(&mut registry);
    let different = reserve(&mut registry);
    let operation = OperationId::from_raw(13);
    let mut bindings = CompletionBindings::new(1);
    assert_eq!(bindings.bind(operation, owned.id), Ok(()));

    assert_eq!(
        bindings.remove_exact(operation, different.id),
        Err(CompletionBindingError::CompletionMismatch)
    );
    assert_eq!(bindings.completion(operation), Some(owned.id));
    assert_eq!(bindings.remove_exact(operation, owned.id), Ok(()));

    rollback(&mut registry, owned);
    rollback(&mut registry, different);
    stop(registry);
}
