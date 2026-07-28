//! Host ordering and position fairness beside one membership action.

use kafka_client_core::Moment;

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    super::{
        classic_group_heartbeat::ClassicHeartbeatExecutionState,
        registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionState,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_legacy_terminal, position_state,
    },
    submission_test::shutdown_driver,
};

#[test]
fn due_heartbeat_preparation_runs_before_but_does_not_starve_position_terminal() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(4))],
    );
    let heartbeat_due = fixture
        .registry
        .entry(fixture.group_id)
        .and_then(|entry| entry.heartbeat.next_deadline())
        .unwrap_or_else(|| panic!("stable assignment heartbeat deadline expected"));
    let clock = MonotonicClock::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));

    let turn = fixture
        .registry
        .turn(Moment::from_tick(heartbeat_due.tick()), &clock, &driver)
        .unwrap_or_else(|error| panic!("registry turn: {error}"));
    assert!(turn.progressed);
    let entry = fixture
        .registry
        .entry(fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    assert!(matches!(
        entry.heartbeat.state(),
        ClassicHeartbeatExecutionState::Prepared(_)
    ));
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));

    fixture
        .registry
        .settle_one_classic_group_position(Moment::from_tick(u64::MAX))
        .unwrap_or_else(|error| panic!("position confirmation: {error:?}"));
    shutdown_driver(&mut driver);
    stop_registry(&mut fixture.registry);
}

#[test]
fn registry_deadline_and_unsettled_totals_include_prepared_position() {
    let mut fixture = super::settlement_test_support::prepared_fixture(&[0]);
    let entry = fixture
        .registry
        .entry(fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let heartbeat_deadline = entry
        .heartbeat
        .next_deadline()
        .unwrap_or_else(|| panic!("heartbeat deadline expected"));
    let processing_deadline = entry
        .processing_lease
        .next_deadline()
        .unwrap_or_else(|| panic!("processing deadline expected"));
    let expected = heartbeat_deadline
        .min(fixture.deadline.core())
        .min(processing_deadline);

    assert_eq!(fixture.registry.position_unsettled(), 1);
    assert_eq!(
        fixture.registry.unsettled(),
        fixture
            .registry
            .membership_unsettled()
            .saturating_add(fixture.registry.processing_unsettled())
            .saturating_add(1)
    );
    assert_eq!(fixture.registry.next_deadline(), Some(expected));

    stop_registry(&mut fixture.registry);
}
