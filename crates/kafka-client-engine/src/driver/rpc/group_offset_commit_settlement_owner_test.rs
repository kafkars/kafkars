//! Exact two-phase terminal-input and route-token owner scenarios.

use kafka_client_core::{
    GroupOffsetCommitEffect, GroupOffsetCommitInput, GroupOffsetCommitMachineError,
    GroupOffsetCommitPartitionOutcome, GroupOffsetCommitState, OperationId, PartitionIndex,
    TopicId,
};

use super::{
    group_offset_commit_calls::TrackedGroupOffsetCommitCalls,
    group_offset_commit_calls_test::{awaiting_machine, result_error},
    group_offset_commit_retry::{
        GroupOffsetCommitBeginError, GroupOffsetCommitConfirmationError, GroupOffsetCommitPoll,
    },
};

#[test]
fn begin_restore_preserves_vec_payload_capacity_and_confirmation_ownership() {
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    let operation_id = OperationId::from_raw(7);
    let mut outcomes = Vec::with_capacity(4);
    outcomes.push(GroupOffsetCommitPartitionOutcome::committed(
        TopicId::from_raw(1),
        PartitionIndex::from_raw(0),
    ));
    calls.install_settlement_for_test(
        operation_id,
        GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 9,
            outcomes,
        },
    );
    assert_eq!(
        calls.confirm_group_commit_settlement(operation_id),
        Err(GroupOffsetCommitConfirmationError::NoPendingConfirmation {
            supplied: operation_id
        })
    );
    assert_eq!(
        calls.begin_group_commit_settlement(OperationId::from_raw(8)),
        Err(GroupOffsetCommitBeginError::OperationMismatch {
            settled: operation_id,
            supplied: OperationId::from_raw(8),
        })
    );
    let first = calls
        .begin_group_commit_settlement(operation_id)
        .unwrap_or_else(|error| panic!("begin exact settlement: {error:?}"));
    assert_broker_input(&first, 4);
    let mut rejecting_machine = awaiting_machine(operation_id);
    let rejected = result_error(
        rejecting_machine.apply(first),
        "awaiting driver rejects broker terminal",
    );
    assert_eq!(rejected.kind(), GroupOffsetCommitMachineError::InvalidState);
    let first = rejected.into_input();
    assert_broker_input(&first, 4);
    calls
        .restore_group_commit_settlement(operation_id, first)
        .unwrap_or_else(|failure| {
            let (_input, error) = failure.into_parts();
            panic!("restore exact settlement: {error:?}");
        });
    assert_eq!(
        calls.poll_group_commit(),
        Ok(GroupOffsetCommitPoll::TerminalReady { operation_id })
    );
    let second = calls
        .begin_group_commit_settlement(operation_id)
        .unwrap_or_else(|error| panic!("begin restored settlement: {error:?}"));
    assert_broker_input(&second, 4);
    let mut accepting_machine = awaiting_machine(operation_id);
    accepting_machine
        .apply(GroupOffsetCommitInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    let transition = accepting_machine
        .apply(second)
        .unwrap_or_else(|error| panic!("core accepts restored terminal: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(GroupOffsetCommitEffect::Complete { .. })
    ));
    assert_eq!(accepting_machine.state(), GroupOffsetCommitState::Completed);
    assert_eq!(
        calls.confirm_group_commit_settlement(OperationId::from_raw(8)),
        Err(GroupOffsetCommitConfirmationError::OperationMismatch {
            pending: operation_id,
            supplied: OperationId::from_raw(8),
        })
    );
    assert_eq!(calls.confirm_group_commit_settlement(operation_id), Ok(()));
    assert_eq!(calls.retained_group_commit_count(), 0);
}

fn assert_broker_input(input: &GroupOffsetCommitInput, expected_capacity: usize) {
    let GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms,
        outcomes,
    } = input
    else {
        panic!("broker response expected");
    };
    assert_eq!(*throttle_time_ms, 9);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes.capacity(), expected_capacity);
}
