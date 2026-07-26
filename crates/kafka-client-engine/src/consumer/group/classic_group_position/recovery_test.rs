//! Post-driver recovery across every position RPC ownership stage.

use std::time::Duration;

use kafka_client_core::{Deadline, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, GroupPositionOffsetFetchCompletionFailureKind},
};

use super::{
    super::{
        classic_group_execution::ClassicGroupExecutionError, registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    ClassicGroupPositionRecoveryFault, ClassicGroupPositionSettlementTurn,
    ClassicGroupPositionSubmissionTurn,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_completion_failure, install_legacy_terminal,
        position_state, prepared_fixture,
    },
};

#[test]
fn active_driver_call_recovers_to_one_local_completion() {
    let mut fixture = prepared_fixture(&[0]);
    let mut driver = driver();
    assert_eq!(
        fixture
            .registry
            .submit_one_classic_group_position(&driver, Moment::from_tick(5)),
        Ok(ClassicGroupPositionSubmissionTurn::Progress)
    );
    shutdown_driver(&mut driver);

    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("active recovery: {error:?}"));
    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("repeated recovery: {error:?}"));
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("completed recovery expected");
    };
    assert_eq!(completed.observed_at(), Moment::from_tick(u64::MAX));
    stop_registry(&mut fixture.registry);
}

#[test]
fn confirmation_pending_receipt_recovers_to_complete() {
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
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));

    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("pending recovery: {error:?}"));
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("completed recovery expected");
    };
    assert_eq!(completed.observed_at(), Moment::from_tick(50));
    stop_registry(&mut fixture.registry);
}

#[test]
fn completion_corruption_recovers_key_then_consumes_observation_on_success() {
    let mut fixture = driver_owned_fixture(&[0]);
    let deadline = fixture.deadline;
    install_completion_failure(
        &mut fixture,
        deadline,
        GroupPositionOffsetFetchCompletionFailureKind::Consumed,
    );

    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("completion recovery: {error:?}"));
    assert!(fixture.registry.position_recovery_fault.is_none());
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Complete(_)
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn completion_reconciliation_failure_retains_distinct_source() {
    let mut fixture = driver_owned_fixture(&[0]);
    let wrong_deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(
        fixture.deadline.core().tick() + 1,
    ));
    install_completion_failure(
        &mut fixture,
        wrong_deadline,
        GroupPositionOffsetFetchCompletionFailureKind::Closed,
    );

    assert_eq!(
        fixture
            .registry
            .recover_classic_group_positions_after_driver_shutdown(),
        Err(ClassicGroupExecutionError::Position(
            ClassicGroupPositionExecutionError::DeadlineMismatch
        ))
    );
    let observation = fixture
        .registry
        .position_recovery_fault
        .as_ref()
        .and_then(ClassicGroupPositionRecoveryFault::completion_observation)
        .unwrap_or_else(|| panic!("completion observation retained"));
    assert_eq!(observation.fence(), fixture.fence);
    assert_eq!(
        observation.kind(),
        GroupPositionOffsetFetchCompletionFailureKind::Closed
    );

    drop(fixture.registry.position_recovery_fault.take());
    drop(fixture.registry.position_shutdown_recovery.take());
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    drop(
        entry
            .position
            .replace(ClassicGroupPositionExecutionState::Dormant),
    );
    stop_registry(&mut fixture.registry);
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build: {error}"))
}

fn shutdown_driver(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}
