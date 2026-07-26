//! Join terminal interpretation at the exact original absolute deadline.

use kafka_client_core::{ClassicBrokerStage, ClassicGroupFatalReason, ClassicGroupPhase, Moment};

use crate::driver::classic_group::install_join_broker_rejection_terminal;

use super::{
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::{
        follower_join_terminal, join_key, prepared_join_terminal,
    },
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

#[test]
fn load_in_progress_arms_the_exact_retained_coordinator_rejoin() {
    let (mut registry, group_id, identity) = prepared_join_terminal();
    install_join_rejection(&mut registry, identity, 14);

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(3)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.rejoin.schedule().is_some());
    assert!(entry.fault.is_none());

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(4)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}

#[test]
fn unknown_join_code_becomes_the_exact_core_fatal() {
    let (mut registry, group_id, identity) = prepared_join_terminal();
    install_join_rejection(&mut registry, identity, 1_234);

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(3)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let fatal = entry
        .classic
        .machine()
        .fatal()
        .unwrap_or_else(|| panic!("fatal broker fact expected"));
    let ClassicGroupFatalReason::Broker { stage, error } = fatal.reason() else {
        panic!("broker fatal expected");
    };
    assert_eq!(stage, ClassicBrokerStage::Join);
    assert_eq!(error.code(), 1_234);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Fatal);
    assert!(entry.rejoin.is_dormant());

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(4)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}

fn install_join_rejection(
    registry: &mut super::registry::GroupConsumerRegistry,
    identity: super::classic_group_join::ClassicGroupJoinIdentity,
    error_code: i16,
) {
    install_join_broker_rejection_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        join_key(identity),
        error_code,
    );
}
