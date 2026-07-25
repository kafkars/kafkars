//! Registry commit selection, checkpoint recovery, and global-capacity scenarios.

use kafka_client_core::{
    DeliveryStatus, GroupId, GroupOffsetCommitAdmissionErrorKind, GroupOffsetCommitFailureKind,
    GroupOffsetCommitTerminal,
};

use super::{
    offset_commit::GroupOffsetCommitAdmissionFailureKind,
    registry_commit::GroupConsumerCommitFailureKind,
    registry_test_support::{
        checkpoint, deadline, install_session, register, started_registry, stop_registry,
    },
};

#[test]
fn caller_group_selects_catalog_and_core_rejects_cross_group_checkpoint() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    install_session(&mut registry, first);
    install_session(&mut registry, second);
    let checkpoint = checkpoint(&registry, first);

    let failure = registry
        .try_commit(second, deadline(100), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("cross-group checkpoint must be rejected"));
    assert_eq!(
        failure.kind,
        GroupConsumerCommitFailureKind::OffsetCommit(GroupOffsetCommitAdmissionFailureKind::Core(
            GroupOffsetCommitAdmissionErrorKind::GroupMismatch
        ))
    );
    assert_eq!(failure.checkpoint.group_id(), first);
    stop_registry(&mut registry);
}

#[test]
fn unknown_group_rejection_retains_checkpoint_without_touching_host() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint(&registry, group_id);
    let unknown =
        GroupId::try_from_raw(999).unwrap_or_else(|| panic!("unknown identity must be nonzero"));

    let failure = registry
        .try_commit(unknown, deadline(100), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("unknown group must be rejected"));
    assert_eq!(failure.kind, GroupConsumerCommitFailureKind::UnknownGroup);
    assert_eq!(failure.checkpoint.group_id(), group_id);
    stop_registry(&mut registry);
}

#[test]
fn one_global_host_bounds_many_groups_and_closing_keeps_accepted_work_owed() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    install_session(&mut registry, first);
    install_session(&mut registry, second);
    let mut accepted = Vec::new();

    for index in 0..8 {
        let group_id = if index % 2 == 0 { first } else { second };
        let group_checkpoint = checkpoint(&registry, group_id);
        let admission = registry
            .try_commit(group_id, deadline(100), group_checkpoint)
            .unwrap_or_else(|failure| panic!("commit {index} failed: {:?}", failure.kind));
        assert!(admission.fault.is_none());
        accepted.push(admission);
    }
    assert_eq!(registry.close_group(first), Ok(()));
    let ninth_checkpoint = checkpoint(&registry, second);
    let ninth = registry
        .try_commit(second, deadline(100), ninth_checkpoint)
        .err()
        .unwrap_or_else(|| panic!("global ninth commit must be rejected"));
    assert_eq!(
        ninth.kind,
        GroupConsumerCommitFailureKind::OffsetCommit(
            GroupOffsetCommitAdmissionFailureKind::Capacity
        )
    );
    assert_eq!(ninth.checkpoint.group_id(), second);

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("driver-shutdown recovery failed: {error}"));
    for admission in accepted {
        let terminal = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("accepted terminal missing: {error}"));
        let GroupOffsetCommitTerminal::Failed(failure) = terminal else {
            panic!("queued recovery must fail definitely unsent");
        };
        assert_eq!(failure.kind(), GroupOffsetCommitFailureKind::DriverRejected);
        assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    }
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown failed: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
}
