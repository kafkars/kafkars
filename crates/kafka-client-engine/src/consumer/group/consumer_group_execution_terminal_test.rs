//! Failure settlement clears one exact join obligation without a revoke.

use std::time::Duration;

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    GroupId, Moment,
};

use crate::clock::MonotonicClock;
use crate::consumer::GroupConsumerStartupFailureKind;

use super::{
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupRediscoveryState},
    consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
    consumer_group_execution_fencing::consumer_group_execution_is_ready,
    consumer_group_execution_terminal::ConsumerGroupRediscoveryDecision,
    consumer_group_heartbeat_settlement::ConsumerGroupHeartbeatSettlementTurn,
    consumer_group_heartbeat_settlement_test::{
        installed_modern_entry, modern_entry_with_instance,
    },
    registry::GroupConsumerRegistry,
};

#[test]
fn registry_exposes_only_a_pre_first_success_modern_terminal() {
    let mut entry = modern_entry_with_instance(None);
    let group_id = entry.group_id();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .apply_current_failure(ConsumerGroupHeartbeatFailure::Compatibility)
        .unwrap_or_else(|error| panic!("startup terminal: {error:?}"));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.consumer_group_startup_failure(group_id),
        Ok(Some(GroupConsumerStartupFailureKind::Compatibility))
    );
}

#[test]
fn terminal_after_first_broker_success_is_not_a_startup_failure() {
    let (mut entry, _topic_id) = installed_modern_entry();
    let group_id = entry.group_id();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("steady heartbeat schedule"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .prepare_due_heartbeat(
            Moment::from_tick(schedule.deadline().tick()),
            &MonotonicClock::new(),
        )
        .unwrap_or_else(|error| panic!("prepare steady heartbeat: {error:?}"));
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .apply_current_failure(ConsumerGroupHeartbeatFailure::Execution)
        .unwrap_or_else(|error| panic!("steady terminal: {error:?}"));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(registry.consumer_group_startup_failure(group_id), Ok(None));
}

#[test]
fn due_first_rediscovery_expires_while_invalidation_remains_blocked() {
    let clock = MonotonicClock::new();
    let mut entry = modern_entry_with_instance(None);
    let group_id = entry.group_id();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let execution = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    execution
        .apply_current_rediscovery(capture.now(), ConsumerGroupHeartbeatFailure::Broker(16))
        .unwrap_or_else(|error| panic!("rediscovery: {error:?}"));
    let schedule = execution
        .machine()
        .retry_schedule()
        .unwrap_or_else(|| panic!("rediscovery schedule"));
    assert_eq!(
        execution
            .prepare_due_coordinator_load_retry(Moment::from_tick(schedule.not_before().tick(),)),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
    );
    assert!(execution.machine().retry_schedule().is_none());
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
    );
    let deadline = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared replacement"))
        .deadline()
        .core();
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.entries.push(entry);
    assert_eq!(
        registry.settle_one_consumer_group_heartbeat(Moment::from_tick(deadline.tick()), &clock,),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert_eq!(
        registry.consumer_group_startup_failure(group_id),
        Ok(Some(GroupConsumerStartupFailureKind::DeadlineElapsed))
    );
}

#[test]
fn due_before_invalidation_terminal_preserves_a_fresh_blocked_replacement() {
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
    let replacement = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared replacement"));
    let schedule = execution
        .machine()
        .retry_schedule()
        .unwrap_or_else(|| panic!("rediscovery retry schedule"));
    assert_ne!(replacement.attempt(), prepared.attempt());
    assert_eq!(replacement.deadline(), prepared.deadline());
    assert_eq!(schedule.attempt(), replacement.attempt());
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
    );
    assert_eq!(
        execution
            .prepare_due_coordinator_load_retry(Moment::from_tick(schedule.not_before().tick(),)),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
    );
    assert!(!consumer_group_execution_is_ready(&execution));

    execution
        .permit_rediscovery_replacement()
        .unwrap_or_else(|error| panic!("invalidation terminal: {error:?}"));
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::ReplacementAdmitted
    );
    assert!(consumer_group_execution_is_ready(&execution));
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
        execution.prepare_due_coordinator_load_retry(deadline),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::Terminal {
            kind: ConsumerGroupHeartbeatRequestKind::Join,
            revoked: None,
        })
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.prepared(), None);
}

#[test]
fn invalidation_terminal_before_due_keeps_leave_blocked_until_its_delay() {
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
    let replacement = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared leave replacement"));
    let schedule = execution
        .machine()
        .retry_schedule()
        .unwrap_or_else(|| panic!("leave retry schedule"));
    assert_ne!(replacement.attempt(), prepared.attempt());
    assert_eq!(replacement.kind(), prepared.kind());
    assert_eq!(replacement.member_id(), prepared.member_id());
    assert_eq!(replacement.member_epoch(), prepared.member_epoch());
    assert_eq!(
        replacement.assignment_generation(),
        prepared.assignment_generation()
    );
    assert_eq!(replacement.deadline(), prepared.deadline());
    assert_eq!(
        execution.rediscovery_state(),
        ConsumerGroupRediscoveryState::AwaitingInvalidationAdmission
    );

    execution
        .permit_rediscovery_replacement()
        .unwrap_or_else(|error| panic!("leave invalidation terminal: {error:?}"));
    assert!(!consumer_group_execution_is_ready(&execution));
    assert_eq!(
        execution
            .prepare_due_coordinator_load_retry(Moment::from_tick(schedule.not_before().tick(),)),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
    );
    assert!(consumer_group_execution_is_ready(&execution));
}
