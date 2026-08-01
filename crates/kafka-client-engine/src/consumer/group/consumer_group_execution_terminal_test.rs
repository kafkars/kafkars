//! Failure settlement clears one exact join obligation without a revoke.

use std::time::Duration;

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    GroupId, Moment,
};

use crate::clock::MonotonicClock;

use super::{
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupRediscoveryState},
    consumer_group_execution_terminal::ConsumerGroupRediscoveryDecision,
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
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
        ConsumerGroupRediscoveryDecision::Terminal {
            revoked: None,
            failure: ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
        }
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
        ConsumerGroupRediscoveryDecision::Terminal {
            revoked: None,
            failure: ConsumerGroupHeartbeatFailure::DeadlineElapsed,
        }
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.prepared(), None);
}

#[test]
fn leave_rediscovery_preserves_prepared_facts_and_revokes_on_bounded_terminal() {
    let (mut entry, _topic_id) = installed_modern_entry();
    let mut execution = entry
        .consumer
        .take()
        .unwrap_or_else(|| panic!("modern execution"));
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    assert_eq!(
        execution.prepare_leave(capture.now(), capture.operation_deadline()),
        Ok(true)
    );
    let prepared = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared leave"));
    assert_eq!(prepared.kind(), ConsumerGroupHeartbeatRequestKind::Leave);

    assert_eq!(
        execution
            .apply_current_rediscovery(capture.now(), ConsumerGroupHeartbeatFailure::Broker(16),)
            .unwrap_or_else(|error| panic!("leave rediscovery: {error:?}")),
        ConsumerGroupRediscoveryDecision::Rediscover
    );
    assert_eq!(execution.prepared(), Some(prepared));
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
    );

    execution
        .permit_rediscovery_replacement()
        .unwrap_or_else(|error| panic!("permit leave replacement: {error:?}"));
    let terminal = execution
        .apply_current_rediscovery(capture.now(), ConsumerGroupHeartbeatFailure::Broker(16))
        .unwrap_or_else(|error| panic!("bounded leave terminal: {error:?}"));
    assert!(matches!(
        terminal,
        ConsumerGroupRediscoveryDecision::Terminal {
            revoked: Some(_),
            failure: ConsumerGroupHeartbeatFailure::Broker(16),
        }
    ));
    assert_eq!(execution.prepared(), None);
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
}
