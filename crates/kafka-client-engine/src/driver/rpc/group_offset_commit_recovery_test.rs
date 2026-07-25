//! Lossless completion and post-driver-shutdown recovery scenarios.

use kafka_client_core::{
    GroupOffsetCommitInput, GroupOffsetCommitPartitionOutcome, OperationId, PartitionIndex, TopicId,
};

use super::{
    group_offset_commit_calls::TrackedGroupOffsetCommitCalls,
    group_offset_commit_calls_test::{prepared, result_error},
};

#[test]
fn completion_corruption_retains_capacity_and_recovers_exact_prepared_owner() {
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    calls
        .install_completion_failure_for_test(prepared(11), kafka_driver::CompletionError::Consumed);
    let observation = result_error(
        calls.poll_group_commit(),
        "completion corruption is observable",
    );
    assert_eq!(observation.operation_id(), OperationId::from_raw(11));
    assert_eq!(
        observation.source(),
        kafka_driver::CompletionError::Consumed
    );
    assert_eq!(calls.retained_group_commit_count(), 1);
    let recovery = calls.recover_group_commits_after_driver_shutdown();
    let (active, settled, pending, completion) = recovery.into_parts();
    assert!(active.is_empty());
    assert!(settled.is_none());
    assert_eq!(pending, None);
    let (prepared, recovered_observation) = completion
        .unwrap_or_else(|| panic!("completion recovery retained"))
        .into_parts();
    assert_eq!(prepared.operation_id(), OperationId::from_raw(11));
    assert_eq!(prepared.entries_capacity(), 1);
    assert_eq!(recovered_observation, observation);
    assert_eq!(calls.retained_group_commit_count(), 0);
}

#[test]
fn post_driver_recovery_preserves_settled_vec_and_empties_registry() {
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    let operation_id = OperationId::from_raw(14);
    let mut outcomes = Vec::with_capacity(4);
    outcomes.push(GroupOffsetCommitPartitionOutcome::committed(
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
    ));
    calls.install_settlement_for_test(
        operation_id,
        GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 3,
            outcomes,
        },
    );
    let recovery = calls.recover_group_commits_after_driver_shutdown();
    let (active, settled, pending, completion) = recovery.into_parts();
    assert!(active.is_empty());
    let (recovered_operation, input) = settled
        .unwrap_or_else(|| panic!("settled input recovered"))
        .into_parts();
    assert_eq!(recovered_operation, operation_id);
    assert_broker_input_capacity(&input, 3, 4);
    assert_eq!(pending, None);
    assert!(completion.is_none());
    assert_eq!(calls.retained_group_commit_count(), 0);
}

#[test]
fn post_driver_recovery_reports_pending_external_input_and_empties_registry() {
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    let operation_id = OperationId::from_raw(15);
    calls.install_settlement_for_test(operation_id, GroupOffsetCommitInput::InvalidResponse);
    let external_input = calls
        .begin_group_commit_settlement(operation_id)
        .unwrap_or_else(|error| panic!("begin pending settlement: {error:?}"));
    let recovery = calls.recover_group_commits_after_driver_shutdown();
    let (active, settled, pending, completion) = recovery.into_parts();
    assert!(active.is_empty());
    assert!(settled.is_none());
    assert_eq!(pending, Some(operation_id));
    assert!(completion.is_none());
    assert_eq!(calls.retained_group_commit_count(), 0);
    assert_eq!(external_input, GroupOffsetCommitInput::InvalidResponse);
}

fn assert_broker_input_capacity(
    input: &GroupOffsetCommitInput,
    expected_throttle: u32,
    expected_capacity: usize,
) {
    let GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms,
        outcomes,
    } = input
    else {
        panic!("broker response expected");
    };
    assert_eq!(*throttle_time_ms, expected_throttle);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes.capacity(), expected_capacity);
}
