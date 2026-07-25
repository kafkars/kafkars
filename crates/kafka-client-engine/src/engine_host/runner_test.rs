//! Host wait selection scenarios.

use std::time::Duration;

use kafka_client_core::{Deadline, Moment};

use crate::producer::host_turn::ProducerTurnOutcome;

use super::{
    assigned_consumer::AssignedConsumerProgress,
    group_consumer::GroupConsumerProgress,
    runner::{assigned_consumer_wait, group_consumer_wait, producer_wait},
};

#[test]
fn prepared_submission_parks_on_its_original_deadline() {
    let outcome = outcome(Some(Deadline::from_tick(250)), false, false);

    assert_eq!(
        producer_wait(Moment::from_tick(100), Some(outcome), false),
        Duration::from_nanos(150)
    );
}

#[test]
fn every_nonzero_wait_is_capped_for_failed_wake_liveness() {
    let outcome = outcome(Some(Deadline::from_tick(1_000_000_000)), false, false);

    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(outcome), false),
        Duration::from_millis(100)
    );
}

#[test]
fn only_runnable_or_driver_local_work_requests_an_immediate_turn() {
    let runnable = outcome(None, true, false);
    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(runnable), false),
        Duration::ZERO
    );
    let idle = outcome(None, false, false);
    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(idle), true),
        Duration::ZERO
    );
}

#[test]
fn transient_lock_or_notification_work_uses_the_liveness_cap() {
    let blocked = outcome(None, false, true);

    assert_eq!(
        producer_wait(Moment::from_tick(0), Some(blocked), false),
        Duration::from_millis(100)
    );
}

#[test]
fn actual_assigned_progress_requests_an_immediate_turn() {
    let progress = assigned_progress(None, true, false);

    assert_eq!(
        assigned_consumer_wait(Moment::from_tick(10), Duration::from_millis(100), &progress),
        Duration::ZERO
    );
}

#[test]
fn assigned_deadline_can_preempt_other_domain_waits() {
    let progress = assigned_progress(Some(Deadline::from_tick(30)), false, false);

    assert_eq!(
        assigned_consumer_wait(Moment::from_tick(10), Duration::from_nanos(80), &progress),
        Duration::from_nanos(20)
    );
}

#[test]
fn elapsed_assigned_deadline_is_immediate_without_claiming_progress() {
    let progress = assigned_progress(Some(Deadline::from_tick(9)), false, false);

    assert_eq!(
        assigned_consumer_wait(Moment::from_tick(10), Duration::from_millis(100), &progress),
        Duration::ZERO
    );
}

#[test]
fn assigned_contention_uses_the_liveness_cap_instead_of_spinning() {
    let progress = assigned_progress(None, false, true);

    assert_eq!(
        assigned_consumer_wait(Moment::from_tick(10), Duration::from_millis(100), &progress),
        Duration::from_millis(100)
    );
}

#[test]
fn group_progress_requests_an_immediate_followup_turn() {
    let progress = group_progress(None, true, false);

    assert_eq!(
        group_consumer_wait(Moment::from_tick(10), Duration::from_millis(100), &progress),
        Duration::ZERO
    );
}

#[test]
fn group_deadline_can_preempt_other_domain_waits() {
    let progress = group_progress(Some(Deadline::from_tick(30)), false, false);

    assert_eq!(
        group_consumer_wait(Moment::from_tick(10), Duration::from_nanos(80), &progress),
        Duration::from_nanos(20)
    );
}

#[test]
fn blocked_group_work_uses_the_liveness_cap_instead_of_spinning() {
    let progress = group_progress(None, false, true);

    assert_eq!(
        group_consumer_wait(Moment::from_tick(10), Duration::from_millis(100), &progress),
        Duration::from_millis(100)
    );
}

const fn outcome(
    next_deadline: Option<Deadline>,
    runnable_work: bool,
    blocked_work: bool,
) -> ProducerTurnOutcome {
    ProducerTurnOutcome {
        batch_timers: 0,
        prepared_effects: 0,
        submission_expiries: 0,
        completion_retries: 0,
        reclaim_attempts: 0,
        next_deadline,
        runnable_work,
        blocked_work,
    }
}

const fn assigned_progress(
    next_deadline: Option<Deadline>,
    progressed: bool,
    blocked_work: bool,
) -> AssignedConsumerProgress {
    AssignedConsumerProgress {
        unsettled: 1,
        progressed,
        blocked_work,
        next_deadline,
        close_completed: false,
    }
}

const fn group_progress(
    next_deadline: Option<Deadline>,
    progressed: bool,
    blocked_work: bool,
) -> GroupConsumerProgress {
    GroupConsumerProgress {
        unsettled: 1,
        progressed,
        blocked_work,
        next_deadline,
    }
}
