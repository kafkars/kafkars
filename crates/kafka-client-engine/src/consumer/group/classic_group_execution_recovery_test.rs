//! Exact Join execution-state reconciliation after driver shutdown.

use kafka_client_core::{GroupId, Moment};

use crate::driver::classic_group::{JoinGroupCallKey, RecoveredJoinGroupOwnership};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::{follower_join_terminal, join_key, leader_join_terminal},
    registry_test_support::stop_registry,
};

#[test]
fn mismatched_leader_recovery_restores_the_exact_deferred_state() {
    let (mut registry, group_id, identity) = leader_join_terminal();
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Blocked)
    );
    let calls = registry
        .join_calls
        .take()
        .unwrap_or_else(|| panic!("Join calls expected"));
    let mut recovery = calls.recover_join_groups_after_driver_shutdown();
    let exact = recovery
        .take_settled()
        .unwrap_or_else(|| panic!("settled Join recovery expected"));
    let wrong_group = GroupId::try_from_raw(group_id.get() + 1)
        .unwrap_or_else(|| panic!("different group identity"));
    let wrong_key = JoinGroupCallKey::new(wrong_group, identity.cycle(), identity.deadline());
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));

    let (error, wrong) = entry
        .execution
        .reconcile_join_after_driver_shutdown(RecoveredJoinGroupOwnership::active_for_test(
            wrong_key,
        ))
        .err()
        .unwrap_or_else(|| panic!("mismatched leader recovery must reject"));
    assert_eq!(error, ClassicGroupExecutionError::HandoffMismatch);
    assert_eq!(wrong.key(), wrong_key);
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::LeaderDeferred(call)
            if call.identity() == identity
    ));
    drop(wrong);

    entry
        .execution
        .reconcile_join_after_driver_shutdown(exact)
        .unwrap_or_else(|(error, _recovered)| panic!("exact leader recovery failed: {error:?}"));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedJoin(prepared)
            if prepared.identity() == identity
    ));
    stop_registry(&mut registry);
}

#[test]
fn pending_follower_confirmation_recovers_the_exact_prepared_sync() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let calls = registry
        .join_calls
        .take()
        .unwrap_or_else(|| panic!("Join calls expected"));
    let mut recovery = calls.recover_join_groups_after_driver_shutdown();
    let recovered = recovery
        .take_pending()
        .unwrap_or_else(|| panic!("pending Join confirmation expected"));
    assert_eq!(recovered.key(), join_key(identity));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending {
            successor: ClassicGroupJoinSuccessor::Sync(_),
            ..
        }
    ));

    entry
        .execution
        .reconcile_join_after_driver_shutdown(recovered)
        .unwrap_or_else(|(error, _recovered)| panic!("pending Join recovery failed: {error:?}"));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(prepared)
            if prepared.group_id() == identity.group_id()
                && prepared.cycle().get() == identity.cycle().get()
                && prepared.deadline() == identity.deadline()
    ));
    stop_registry(&mut registry);
}
