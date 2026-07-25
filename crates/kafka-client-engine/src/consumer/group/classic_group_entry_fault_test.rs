//! Linear first-fault ownership and shutdown accounting scenarios.

use kafka_client_core::{AssignmentGeneration, LiveGroupAssignment};

use crate::driver::classic_group::install_follower_join_terminal;

use super::{
    classic_group_assignment::{
        ClassicGroupAssignmentPreparationFailure, ClassicGroupAssignmentPreparationFailureKind,
    },
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_join_settlement_test::follower_join_terminal,
    classic_group_sync_settlement_test::install_assignment_terminal,
    classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
    registry_test_support::stop_registry,
};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn one_successor_fault_is_one_shutdown_obligation() {
    let fault = ClassicGroupEntryFault::JoinSuccessor(ClassicGroupJoinSuccessor::Idle);

    assert_eq!(fault.retained_owner_count(), 1);
}

#[test]
fn successor_plus_failed_terminal_restore_counts_both_linear_owners() {
    let (mut registry, group_id, _identity) = follower_join_terminal();
    let entry = registry
        .entries
        .iter()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let accepted = entry
        .execution
        .join_call()
        .unwrap_or_else(|| panic!("driver-owned Join expected"))
        .accepted();
    let calls = registry
        .join_calls
        .as_mut()
        .unwrap_or_else(|| panic!("Join calls expected"));
    let terminal = calls
        .begin_join_group_settlement(accepted)
        .unwrap_or_else(|error| panic!("Join settlement failed: {error:?}"));
    install_follower_join_terminal(calls, terminal.key());
    let failure = calls
        .restore_join_group_settlement(terminal)
        .err()
        .unwrap_or_else(|| panic!("occupied settlement must retain raw terminal"));
    let fault = ClassicGroupEntryFault::JoinSuccessorRestore {
        successor: ClassicGroupJoinSuccessor::Idle,
        failure,
    };

    assert_eq!(fault.retained_owner_count(), 2);
}

#[test]
fn the_entry_fault_owner_remains_linear() {
    assert_not_impl!(ClassicGroupEntryFault: Clone);
    assert_not_impl!(ClassicGroupEntryFault: Copy);
}

#[test]
fn join_post_core_fault_counts_terminal_execution_and_pending_route() {
    let (mut registry, group_id, _identity) = follower_join_terminal();
    let (entries, calls) = (&mut registry.entries, &mut registry.join_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Join calls expected"));
    let terminal = calls
        .begin_join_group_settlement(
            entry
                .execution
                .join_call()
                .unwrap_or_else(|| panic!("driver-owned Join expected"))
                .accepted(),
        )
        .unwrap_or_else(|error| panic!("Join settlement failed: {error:?}"));
    entry.fault = Some(ClassicGroupEntryFault::JoinPostCore(terminal));

    assert_eq!(registry.membership_unsettled(), 3);

    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let Some(ClassicGroupEntryFault::JoinPostCore(terminal)) = entry.fault.take() else {
        panic!("Join post-core fault expected");
    };
    registry
        .join_calls
        .as_mut()
        .unwrap_or_else(|| panic!("Join calls expected"))
        .restore_join_group_settlement(terminal)
        .unwrap_or_else(|_failure| panic!("Join terminal restoration failed"));
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}

#[test]
fn sync_install_fault_counts_failure_terminal_execution_and_pending_route() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    let (entries, calls) = (&mut registry.entries, &mut registry.sync_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Sync calls expected"));
    let terminal = calls
        .begin_sync_group_settlement(
            entry
                .execution
                .sync_driver_owner()
                .unwrap_or_else(|| panic!("driver-owned Sync expected"))
                .accepted(),
        )
        .unwrap_or_else(|error| panic!("Sync settlement failed: {error:?}"));
    let assignment = LiveGroupAssignment::try_new(
        group_id,
        identity.member_id(),
        AssignmentGeneration::try_from_raw(1)
            .unwrap_or_else(|| panic!("assignment generation expected")),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("test assignment failed: {error:?}"));
    entry.fault = Some(ClassicGroupEntryFault::SyncInstall {
        failure: ClassicGroupAssignmentPreparationFailure {
            kind: ClassicGroupAssignmentPreparationFailureKind::MissingCandidate,
            assignment,
        },
        generation: identity.generation(),
        terminal,
    });

    assert_eq!(registry.membership_unsettled(), 4);

    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let Some(ClassicGroupEntryFault::SyncInstall { terminal, .. }) = entry.fault.take() else {
        panic!("Sync install fault expected");
    };
    registry
        .sync_calls
        .as_mut()
        .unwrap_or_else(|| panic!("Sync calls expected"))
        .restore_sync_group_settlement(terminal)
        .unwrap_or_else(|_failure| panic!("Sync terminal restoration failed"));
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("registered entry expected"))
            .execution
            .borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    stop_registry(&mut registry);
}
