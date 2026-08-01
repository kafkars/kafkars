//! Local assignment cycles advance only when KIP-848 replaces an assignment.

use std::time::Duration;

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, GroupId, Moment,
};

use crate::clock::MonotonicClock;

use super::{
    consumer_group_execution::ConsumerGroupExecution,
    consumer_group_execution_cadence::ConsumerGroupCoordinatorLoadRetryTurn,
    consumer_group_heartbeat_due::settle_consumer_group_load_retry_turn,
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
    consumer_group_heartbeat_submission::consumer_group_heartbeat_is_ready,
};

#[test]
fn replacement_advances_the_fetch_fence_without_changing_initial_install() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let mut execution = ConsumerGroupExecution::new(group_id);
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let initial = execution
        .next_reconcile_cycle(false)
        .unwrap_or_else(|| panic!("initial cycle"));
    let next = execution
        .next_reconcile_cycle(true)
        .unwrap_or_else(|| panic!("next cycle"));
    assert_eq!(initial.get(), 1);
    assert_eq!(next.get(), 2);
    execution.commit_reconcile_cycle(next);
    assert_eq!(execution.cycle(), Some(next));
}

#[test]
fn coordinator_load_wait_retains_the_exact_prepared_attempt_and_wake_deadline() {
    let (mut entry, now) = prepared_steady_entry();
    let prepared = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared steady heartbeat"));
    let turn = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .schedule_current_coordinator_load_retry(now, ConsumerGroupHeartbeatFailure::Broker(14))
        .unwrap_or_else(|error| panic!("schedule coordinator load retry: {error:?}"));
    let ConsumerGroupCoordinatorLoadRetryTurn::Scheduled { schedule } = turn else {
        panic!("positive coordinator load retry schedule")
    };
    let execution = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("consumer execution"));
    assert_eq!(execution.prepared(), Some(prepared));
    assert_eq!(schedule.attempt(), prepared.attempt());
    assert_eq!(schedule.kind(), prepared.kind());
    assert_eq!(schedule.deadline(), prepared.deadline().core());
    assert!(schedule.not_before().tick() > now.tick());
    assert!(schedule.not_before().tick() < schedule.deadline().tick());
    assert_eq!(execution.next_deadline(), Some(schedule.not_before()));
    assert!(!consumer_group_heartbeat_is_ready(&entry));

    assert_eq!(
        entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("consumer execution"))
            .prepare_due_coordinator_load_retry(now),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::Idle)
    );
    assert_eq!(
        entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("consumer execution"))
            .prepare_due_coordinator_load_retry(Moment::from_tick(schedule.not_before().tick())),
        Ok(ConsumerGroupCoordinatorLoadRetryTurn::SubmissionReady)
    );
    let execution = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("consumer execution"));
    assert_eq!(execution.prepared(), Some(prepared));
    assert_eq!(execution.machine().in_flight(), Some(prepared.attempt()));
    assert!(execution.machine().retry_schedule().is_none());
    assert!(consumer_group_heartbeat_is_ready(&entry));
}

#[test]
fn coordinator_load_retry_due_at_the_original_deadline_terminalizes_and_revokes() {
    let (mut entry, _now) = prepared_steady_entry();
    let deadline = entry
        .consumer
        .as_ref()
        .and_then(ConsumerGroupExecution::prepared)
        .unwrap_or_else(|| panic!("prepared steady heartbeat"))
        .deadline()
        .core();
    let response_now = Moment::from_tick(deadline.tick() - 50_000_000);
    let scheduled = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .schedule_current_coordinator_load_retry(
            response_now,
            ConsumerGroupHeartbeatFailure::Broker(14),
        )
        .unwrap_or_else(|error| panic!("schedule capped retry: {error:?}"));
    assert!(matches!(
        scheduled,
        ConsumerGroupCoordinatorLoadRetryTurn::Scheduled { schedule }
            if schedule.not_before() == deadline
    ));
    let terminal = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .prepare_due_coordinator_load_retry(Moment::from_tick(deadline.tick()))
        .unwrap_or_else(|error| panic!("expire original deadline: {error:?}"));
    assert!(matches!(
        &terminal,
        ConsumerGroupCoordinatorLoadRetryTurn::Terminal {
            kind: kafka_client_core::ConsumerGroupHeartbeatRequestKind::Steady,
            revoked: Some(_),
        }
    ));
    settle_consumer_group_load_retry_turn(&mut entry, terminal)
        .unwrap_or_else(|error| panic!("settle terminal retry: {error:?}"));
    let execution = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("consumer execution"));
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert!(execution.prepared().is_none());
    assert!(execution.machine().retry_schedule().is_none());
    assert!(entry.consumer_revocation.is_some());
}

fn prepared_steady_entry() -> (super::registry_entry::GroupConsumerEntry, Moment) {
    let (mut entry, _topic_id) = installed_modern_entry();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.machine().schedule())
        .unwrap_or_else(|| panic!("steady heartbeat schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("consumer execution"))
        .prepare_due_heartbeat(now, &MonotonicClock::new())
        .unwrap_or_else(|error| panic!("prepare steady heartbeat: {error:?}"));
    (entry, now)
}
