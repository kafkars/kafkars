//! Host wait selection scenarios for runnable, blocked, and driver work.

use std::time::Duration;

use kafka_client_core::{Deadline, Moment};

use crate::producer::ProducerTurnOutcome;

use super::runner::producer_wait;

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
