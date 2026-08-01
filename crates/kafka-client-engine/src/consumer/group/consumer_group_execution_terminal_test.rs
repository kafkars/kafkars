//! Failure settlement clears one exact join obligation without a revoke.

use std::time::Duration;

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, GroupId, Moment,
};

use crate::clock::MonotonicClock;

use super::{
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupRediscoveryState},
    consumer_group_execution_terminal::ConsumerGroupRediscoveryDecision,
};

#[test]
fn initial_failure_becomes_one_retained_fatal_without_assignment_loss() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert_eq!(
        execution
            .apply_current_failure(ConsumerGroupHeartbeatFailure::Compatibility)
            .unwrap_or_else(|error| panic!("failure: {error:?}")),
        None
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.unsettled(), 0);
}

#[test]
fn rediscovery_gate_preserves_prepared_join_until_one_replacement_is_admitted() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let prepared = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared join"));

    assert_eq!(
        execution
            .apply_current_rediscovery(
                capture.now(),
                ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
            )
            .unwrap_or_else(|error| panic!("rediscover: {error:?}")),
        ConsumerGroupRediscoveryDecision::Rediscover
    );
    assert_eq!(execution.prepared(), Some(prepared));
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
    );

    execution
        .permit_rediscovery_replacement()
        .unwrap_or_else(|error| panic!("permit replacement: {error:?}"));
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::ReplacementAdmitted
    );
    assert_eq!(
        execution
            .apply_current_rediscovery(
                capture.now(),
                ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
            )
            .unwrap_or_else(|error| panic!("bounded terminal: {error:?}")),
        ConsumerGroupRediscoveryDecision::Terminal { revoked: None }
    );
    assert_eq!(execution.prepared(), None);
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::Open
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
}

#[test]
fn queued_rediscovery_expires_at_the_original_join_deadline() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert_eq!(
        execution
            .apply_current_rediscovery(
                capture.now(),
                ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
            )
            .unwrap_or_else(|error| panic!("rediscover: {error:?}")),
        ConsumerGroupRediscoveryDecision::Rediscover
    );

    let deadline = Moment::from_tick(capture.deadline().tick());
    assert_eq!(
        execution
            .apply_current_rediscovery(
                deadline,
                ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
            )
            .unwrap_or_else(|error| panic!("deadline terminal: {error:?}")),
        ConsumerGroupRediscoveryDecision::Terminal { revoked: None }
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.prepared(), None);
}
