//! Driver-owned close blocking and impossible handoff close scenarios.

use kafka_client_core::Moment;

use super::{
    super::{
        classic_group_execution::ClassicGroupExecutionError,
        registry_membership::GroupConsumerMembershipTurn, registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    ClassicGroupPositionSettlementTurn,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_legacy_terminal, position_state,
        prepared_fixture,
    },
};

#[test]
fn driver_owned_close_blocks_until_raw_terminal_recovery() {
    let mut fixture = driver_owned_fixture(&[0]);
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(20)),
        Ok(GroupConsumerMembershipTurn::Blocked)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::DriverOwned(_)
    ));

    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(4))],
    );
    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("settled recovery: {error:?}"));
    stop_registry(&mut fixture.registry);
}

#[test]
fn confirmation_pending_close_blocks_until_receipt_recovery() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(4))],
    );
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(30)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(31)),
        Ok(GroupConsumerMembershipTurn::Blocked)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));

    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("pending recovery: {error:?}"));
    stop_registry(&mut fixture.registry);
}

#[test]
fn handoff_close_reports_invariant_failure_without_losing_owner() {
    let mut fixture = prepared_fixture(&[0]);
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let (_key, request) = entry
        .position
        .begin_handoff()
        .unwrap_or_else(|error| panic!("position handoff: {error:?}"));
    drop(request);
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(40)),
        Err(ClassicGroupExecutionError::Position(
            ClassicGroupPositionExecutionError::HandoffIncomplete
        ))
    );
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Handoff(_)
    ));
    drop(entry.fault.take());
    drop(
        entry
            .position
            .replace(ClassicGroupPositionExecutionState::Dormant),
    );
    stop_registry(&mut fixture.registry);
}
