//! Completion corruption and complete shutdown-state recovery scenarios.

use kafka_client_core::Deadline;

use super::{
    admission::GroupPositionOffsetFetchAccepted,
    calls::TrackedGroupPositionOffsetFetchCalls,
    calls_test::{fence, key},
    recovery::GroupPositionOffsetFetchCompletionFailureKind,
};

#[test]
fn completion_corruption_retains_capacity_and_recovers_the_exact_key() {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    calls
        .install_completion_failure_for_test(key(10, 211), kafka_driver::CompletionError::Consumed);
    let observation = calls
        .poll_group_position_offset_fetch()
        .err()
        .unwrap_or_else(|| panic!("completion corruption must be observable"));
    assert_eq!(observation.fence(), fence(10));
    assert_eq!(
        observation.kind(),
        GroupPositionOffsetFetchCompletionFailureKind::Consumed
    );
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 1);

    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    assert!(recovery.pop_active().is_none());
    assert!(recovery.take_settled().is_none());
    assert_eq!(recovery.pending_fence(), None);
    let (recovered_key, recovered_observation) = recovery
        .take_completion()
        .unwrap_or_else(|| panic!("completion owner recovered"))
        .into_parts();
    assert_eq!(recovered_key.fence(), fence(10));
    assert_eq!(
        recovered_key.operation_deadline().core(),
        Deadline::from_tick(211)
    );
    assert_eq!(recovered_observation, observation);
    assert!(recovery.is_empty());
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 0);
}

#[test]
fn shutdown_recovery_distinguishes_settled_from_externally_pending_terminal() {
    let mut settled_calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    settled_calls.install_terminal_for_test(
        key(11, 220),
        Some(9),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );
    let mut recovery = settled_calls.recover_group_position_offset_fetches_after_driver_shutdown();
    assert!(recovery.pop_active().is_none());
    assert_eq!(
        recovery
            .take_settled()
            .as_ref()
            .unwrap_or_else(|| panic!("settled terminal recovered"))
            .key()
            .fence(),
        fence(11)
    );
    assert_eq!(recovery.pending_fence(), None);
    assert!(recovery.take_completion().is_none());
    assert!(recovery.is_empty());

    let mut pending_calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    pending_calls.install_terminal_for_test(
        key(12, 230),
        Some(8),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );
    let accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(12));
    let external = pending_calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("begin external settlement: {error:?}"));
    let mut recovery = pending_calls.recover_group_position_offset_fetches_after_driver_shutdown();
    assert!(recovery.pop_active().is_none());
    assert!(recovery.take_settled().is_none());
    assert_eq!(recovery.pending_fence(), Some(fence(12)));
    assert!(recovery.take_completion().is_none());
    assert_eq!(external.key().fence(), accepted.fence());
    recovery.clear_pending_fence();
    assert!(recovery.is_empty());
    accepted.confirm_receipt();
    assert_eq!(
        pending_calls.retained_group_position_offset_fetch_count(),
        0
    );
}
