//! Transaction topic-binding and validation-seal revision scenarios.

use crate::{ErrorKind, TopicUuid};

use super::identity::TransactionIdentityState;

#[test]
fn accepted_mutations_commit_exact_bindings_and_invalidate_the_seal() {
    let mut state = TransactionIdentityState::new();
    let uuid = topic_uuid(1);
    let prepared = state
        .prepare_mutation(Some(("orders", Some(uuid))))
        .unwrap_or_else(|error| panic!("prepare binding failed: {error}"));

    assert!(state.topics().is_empty());
    assert!(!state.requires_validation());
    state.commit_mutation(prepared);
    assert_eq!(state.revision(), 1);
    assert_eq!(state.topics()[0].topic(), "orders");
    assert_eq!(state.topics()[0].topic_uuid(), Some(uuid));
    assert!(!state.is_sealed());

    state
        .install_seal(1)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    assert!(state.is_sealed());
    let prepared = state
        .prepare_mutation(None)
        .unwrap_or_else(|error| panic!("prepare mutation failed: {error}"));
    assert!(state.is_sealed());
    state.commit_mutation(prepared);
    assert_eq!(state.revision(), 2);
    assert!(!state.is_sealed());
}

#[test]
fn repeated_exact_binding_is_stable_while_conflicting_uuid_rejects() {
    let mut state = TransactionIdentityState::new();
    let first = state
        .prepare_mutation(Some(("orders", Some(topic_uuid(1)))))
        .unwrap_or_else(|error| panic!("first binding failed: {error}"));
    state.commit_mutation(first);

    let repeated = state
        .prepare_mutation(Some(("orders", Some(topic_uuid(1)))))
        .unwrap_or_else(|error| panic!("repeated binding failed: {error}"));
    state.commit_mutation(repeated);
    assert_eq!(state.topics().len(), 1);
    assert_eq!(state.revision(), 2);

    let error = state
        .prepare_mutation(Some(("orders", Some(topic_uuid(2)))))
        .err()
        .unwrap_or_else(|| panic!("conflicting topic UUID must reject"));
    assert_eq!(error.kind(), ErrorKind::Identity);
    assert_eq!(state.topics().len(), 1);
    assert_eq!(state.revision(), 2);
}

#[test]
fn stale_validation_snapshot_cannot_install_a_commit_seal() {
    let mut state = TransactionIdentityState::new();
    let first = state
        .prepare_mutation(Some(("orders", Some(topic_uuid(1)))))
        .unwrap_or_else(|error| panic!("binding failed: {error}"));
    state.commit_mutation(first);
    let stale_revision = state.revision();
    let second = state
        .prepare_mutation(None)
        .unwrap_or_else(|error| panic!("second mutation failed: {error}"));
    state.commit_mutation(second);

    let error = state
        .install_seal(stale_revision)
        .err()
        .unwrap_or_else(|| panic!("stale validation must not seal"));
    assert_eq!(error.kind(), ErrorKind::State);
    assert!(!state.is_sealed());
}

#[test]
fn bound_and_unbound_uses_of_one_topic_reject_in_both_directions() {
    for (first, second) in [(None, Some(topic_uuid(1))), (Some(topic_uuid(1)), None)] {
        let mut state = TransactionIdentityState::new();
        let accepted = state
            .prepare_mutation(Some(("orders", first)))
            .unwrap_or_else(|error| panic!("first identity mode prepares: {error}"));
        state.commit_mutation(accepted);
        let revision = state.revision();

        let error = state
            .prepare_mutation(Some(("orders", second)))
            .err()
            .unwrap_or_else(|| panic!("mixed identity modes must reject"));

        assert_eq!(error.kind(), ErrorKind::Identity);
        assert_eq!(state.revision(), revision);
        assert_eq!(state.topics().len(), 1);
        assert_eq!(state.topics()[0].topic_uuid(), first);
    }
}

fn topic_uuid(last: u8) -> TopicUuid {
    let mut bytes = [0_u8; 16];
    bytes[15] = last;
    TopicUuid::try_from_bytes(bytes).unwrap_or_else(|| panic!("nonzero UUID"))
}
