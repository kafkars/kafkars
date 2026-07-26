//! Exact Join confirmation restoration when rediscovery route evidence is absent.

use kafka_client_core::Moment;

use crate::driver::classic_group::{JoinGroupPoll, install_join_broker_rejection_terminal};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::{join_key, prepared_join_terminal},
    registry_test_support::stop_registry,
};

#[test]
fn unavailable_join_route_token_preserves_both_confirmation_owners() {
    let (mut registry, group_id, identity) = prepared_join_terminal();
    let key = join_key(identity);
    install_join_broker_rejection_terminal(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected")),
        key,
        15,
    );

    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(3)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(4)),
        Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.rediscovery.awaits_route_transfer());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinConfirmationPending { call, .. }
            if call.accepted().key() == key
    ));
    assert_eq!(
        registry
            .join_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Join calls expected"))
            .poll_join_group(),
        Ok(JoinGroupPoll::ConfirmationPending { key })
    );
    assert_eq!(
        registry
            .coordinator_invalidations
            .as_ref()
            .unwrap_or_else(|| panic!("invalidation owner expected"))
            .retained_count(),
        0
    );

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}
