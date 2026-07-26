//! Completion observations remain orthogonal to the failing recovery owner.

use kafka_client_core::Deadline;

use crate::{
    clock::OperationDeadline,
    driver::{
        GroupPositionOffsetFetchCompletionFailureKind, GroupPositionOffsetFetchKey,
        TrackedGroupPositionOffsetFetchCalls,
    },
};

use super::{
    ClassicGroupPositionExecutionError, ClassicGroupPositionRecoveryFault,
    settlement_test_support::prepared_fixture,
};

#[test]
fn completion_observation_wraps_a_non_key_recovery_fault_without_reclassification() {
    let fixture = prepared_fixture(&[0]);
    let key = GroupPositionOffsetFetchKey::new(
        fixture.fence,
        OperationDeadline::from_core_for_test(Deadline::from_tick(300)),
    );
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(1);
    calls.install_completion_failure_kind_for_test(
        key,
        GroupPositionOffsetFetchCompletionFailureKind::Closed,
    );
    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    let (_key, observation) = recovery
        .take_completion()
        .unwrap_or_else(|| panic!("completion recovery expected"))
        .into_parts();
    let fault = ClassicGroupPositionRecoveryFault::missing_fence(
        ClassicGroupPositionExecutionError::TerminalEffect,
        fixture.fence,
    )
    .with_completion(observation);

    assert_eq!(
        fault.error(),
        ClassicGroupPositionExecutionError::TerminalEffect
    );
    assert_eq!(
        fault
            .completion_observation()
            .unwrap_or_else(|| panic!("completion source expected"))
            .kind(),
        GroupPositionOffsetFetchCompletionFailureKind::Closed
    );
    assert_eq!(fault.retained_owner_count(), 1);
    assert!(recovery.is_empty());

    let mut registry = fixture.registry;
    super::super::registry_test_support::stop_registry(&mut registry);
}
