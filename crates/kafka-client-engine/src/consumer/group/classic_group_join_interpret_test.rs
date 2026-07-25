//! Join terminal interpretation at the exact original absolute deadline.

use kafka_client_core::{ClassicGroupPhase, Moment};

use super::{
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::follower_join_terminal,
    registry_test_support::stop_registry,
};

#[test]
fn late_success_terminalizes_once_without_staging_or_replaying_sync() {
    let (mut registry, group_id, identity) = follower_join_terminal();
    let deadline = Moment::from_tick(identity.deadline().core().tick());

    assert_eq!(
        registry.settle_one_classic_join(deadline),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.classic.pending().is_none());
    assert!(entry.fault.is_none());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending {
            successor: ClassicGroupJoinSuccessor::Idle,
            ..
        }
    ));

    assert_eq!(
        registry.settle_one_classic_join(deadline),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
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
