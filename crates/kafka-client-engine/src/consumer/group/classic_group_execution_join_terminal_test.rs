//! Join leader deferral and exact two-phase confirmation scenarios.

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_settlement_test::{follower_join_terminal, join_key},
    registry_test_support::stop_registry,
};

#[test]
fn exact_pending_route_confirmation_finishes_to_the_staged_successor() {
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
    drop(terminal);
    entry
        .execution
        .stage_join_confirmation(ClassicGroupJoinSuccessor::Idle)
        .unwrap_or_else(|(error, _successor)| panic!("Join stage failed: {error:?}"));

    entry
        .execution
        .confirm_join(calls)
        .unwrap_or_else(|error| panic!("Join confirmation failed: {error:?}"));

    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    assert_eq!(calls.retained_join_group_count(), 0);
    stop_registry(&mut registry);
}

#[test]
fn missing_pending_route_preserves_the_exact_receipt_and_successor() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    let (entries, calls) = (&mut registry.entries, &mut registry.join_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Join calls expected"));
    entry
        .execution
        .stage_join_confirmation(ClassicGroupJoinSuccessor::Idle)
        .unwrap_or_else(|(error, _successor)| panic!("Join stage failed: {error:?}"));

    assert_eq!(
        entry.execution.confirm_join(calls),
        Err(ClassicGroupExecutionError::CallIdentityMismatch)
    );
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending {
            call,
            successor: ClassicGroupJoinSuccessor::Idle,
        } if call.accepted().key() == join_key(identity)
    ));

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}

#[test]
fn leader_deferral_retains_the_exact_driver_owner() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));

    entry
        .execution
        .defer_join_leader()
        .unwrap_or_else(|error| panic!("leader defer failed: {error:?}"));

    assert!(entry.execution.join_is_deferred());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::LeaderDeferred(call)
            if call.identity() == identity
    ));
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}
