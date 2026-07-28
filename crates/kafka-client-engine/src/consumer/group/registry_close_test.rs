//! Per-group close isolation and whole-registry shutdown scenarios.

use kafka_client_core::{GroupOffsetCommitAdmissionErrorKind, Moment};

use super::{
    offset_commit::GroupOffsetCommitAdmissionFailureKind,
    registry_close::GroupConsumerCloseError,
    registry_commit::GroupConsumerCommitFailureKind,
    registry_host_error::GroupConsumerHostError,
    registry_membership::GroupConsumerMembershipTurn,
    registry_test_support::{
        checkpoint, deadline, install_session, register, started_registry, stop_registry,
    },
};

#[test]
fn closing_one_group_does_not_close_global_commit_admission() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    install_session(&mut registry, first);
    install_session(&mut registry, second);
    let first_checkpoint = checkpoint(&registry, first);
    assert_eq!(registry.close_group(first), Ok(()));

    let failure = registry
        .try_commit(first, deadline(100), first_checkpoint)
        .err()
        .unwrap_or_else(|| panic!("closing group must reject new commit"));
    assert_eq!(failure.kind, GroupConsumerCommitFailureKind::GroupClosing);
    assert!(registry.accepting);
    assert!(
        registry
            .entry(second)
            .is_some_and(super::registry_entry::GroupConsumerEntry::is_active)
    );

    let first_checkpoint = checkpoint(&registry, first);
    let second_failure = registry
        .try_commit(second, deadline(100), first_checkpoint)
        .err()
        .unwrap_or_else(|| panic!("cross-group checkpoint must be rejected"));
    assert_eq!(
        second_failure.kind,
        GroupConsumerCommitFailureKind::OffsetCommit(GroupOffsetCommitAdmissionFailureKind::Core(
            GroupOffsetCommitAdmissionErrorKind::GroupMismatch
        ))
    );
    stop_registry(&mut registry);
}

#[test]
fn whole_registry_close_fences_every_entry_and_global_host_once() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    registry.close_admission();

    assert!(!registry.accepting);
    assert!(registry.entries.iter().all(|entry| !entry.is_active()));
    assert_eq!(
        registry.close_group(group_id),
        Err(GroupConsumerCloseError::AlreadyClosing)
    );
    let shutdown_error = registry
        .finish_shutdown()
        .err()
        .unwrap_or_else(|| panic!("unsettled membership must block shutdown"));
    let expected = GroupConsumerHostError::membership_unsettled(2);
    assert_eq!(shutdown_error, expected);
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(u64::MAX)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown failed: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
}
