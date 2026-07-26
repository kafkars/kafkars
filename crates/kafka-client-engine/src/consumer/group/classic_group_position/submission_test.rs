//! Sync gating and lossless position RPC admission scenarios.

use std::time::Duration;

use kafka_client_core::{
    GroupPositionBootstrapFailureKind, GroupPositionBootstrapState, GroupPositionBootstrapTerminal,
    Moment,
};

use crate::{
    EngineConfig,
    driver::{DriverOwner, TrackedGroupPositionOffsetFetchCalls},
};

use super::{
    super::{
        classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
        classic_group_sync_settlement_test::install_assignment_terminal,
        classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
        registry::GroupConsumerRegistry,
        registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionState, ClassicGroupPositionSubmissionTurn,
};

#[test]
fn sync_confirmation_is_a_hard_submission_gate() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .position
            .state(),
        ClassicGroupPositionExecutionState::Prepared(_)
    ));

    let mut driver = driver();
    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(4)),
        Ok(ClassicGroupPositionSubmissionTurn::Idle)
    );
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

#[test]
fn capacity_returns_the_exact_prepared_request_and_blocks() {
    let (mut registry, group_id, _identity) = confirmed_registry();
    registry.position_calls = Some(TrackedGroupPositionOffsetFetchCalls::new(0));
    let expected = prepared_identity(&registry, group_id);
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(5)),
        Ok(ClassicGroupPositionSubmissionTurn::Blocked)
    );
    assert_eq!(prepared_identity(&registry, group_id), expected);
    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

#[test]
fn elapsed_prepared_request_settles_before_saturated_capacity() {
    let (mut registry, group_id, _identity) = confirmed_registry();
    registry.position_calls = Some(TrackedGroupPositionOffsetFetchCalls::new(0));
    let deadline = prepared_identity(&registry, group_id).1.core();
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(deadline.tick()),),
        Ok(ClassicGroupPositionSubmissionTurn::Progress)
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .position
            .state(),
        ClassicGroupPositionExecutionState::Dormant
    ));
    assert_eq!(
        registry
            .position_calls
            .as_ref()
            .unwrap_or_else(|| panic!("position calls expected"))
            .retained_group_position_offset_fetch_count(),
        0
    );
    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

#[test]
fn acceptance_retains_receipt_beside_submitted_core() {
    let (mut registry, group_id, _identity) = confirmed_registry();
    let expected = prepared_identity(&registry, group_id);
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(5)),
        Ok(ClassicGroupPositionSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::DriverOwned(owner)
            if owner.core_state() == GroupPositionBootstrapState::Submitted
                && owner.accepted().fence() == expected.0
    ));

    shutdown_driver(&mut driver);
    let mut calls = registry
        .position_calls
        .take()
        .unwrap_or_else(|| panic!("position calls expected"));
    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    let key = recovery
        .pop_active()
        .unwrap_or_else(|| panic!("active position key expected"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let state = entry
        .position
        .replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned position expected");
    };
    let (_machine, _correlation, accepted, _result_buffer) = owner.into_parts();
    assert_eq!(key.fence(), accepted.fence());
    drop((key, accepted));
    assert!(recovery.is_empty());
    stop_registry(&mut registry);
}

#[test]
fn local_rejection_before_deadline_completes_as_driver_rejected() {
    let (mut registry, group_id, _identity) = confirmed_registry();
    let mut driver = driver();
    driver
        .close_admission()
        .unwrap_or_else(|error| panic!("driver close admission failed: {error}"));
    driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("driver close turn failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_group_position(&driver, Moment::from_tick(5)),
        Ok(ClassicGroupPositionSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Complete(completed)
            if matches!(
                completed.terminal(),
                GroupPositionBootstrapTerminal::Failed(failure)
                    if failure.kind() == GroupPositionBootstrapFailureKind::DriverRejected
            )
    ));
    assert_eq!(
        registry
            .position_calls
            .as_ref()
            .unwrap_or_else(|| panic!("position calls expected"))
            .retained_group_position_offset_fetch_count(),
        0
    );
    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

pub(super) fn confirmed_registry() -> (
    GroupConsumerRegistry,
    kafka_client_core::GroupId,
    super::super::classic_group_sync::ClassicGroupSyncIdentity,
) {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    (registry, group_id, identity)
}

pub(super) fn prepared_identity(
    registry: &GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) -> (
    kafka_client_core::GroupPositionFence,
    crate::clock::OperationDeadline,
) {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let ClassicGroupPositionExecutionState::Prepared(prepared) = entry.position.state() else {
        panic!("prepared position expected");
    };
    (prepared.key().fence(), prepared.key().operation_deadline())
}

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"))
}

pub(super) fn shutdown_driver(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
}
