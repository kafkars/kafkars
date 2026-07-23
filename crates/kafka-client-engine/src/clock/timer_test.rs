//! Generation fencing and deterministic batch-timer ordering scenarios.

use kafka_client_core::{BatchId, BatchTimerGeneration, Deadline, Moment};

use super::{BatchTimerError, BatchTimers};

fn batch(value: u64) -> BatchId {
    BatchId::from_raw(value)
}

fn generation(value: u64) -> BatchTimerGeneration {
    BatchTimerGeneration::from_raw(value)
}

fn deadline(value: u64) -> Deadline {
    Deadline::from_tick(value)
}

#[test]
fn newer_generation_replaces_older_schedule_once() {
    let mut timers = BatchTimers::new(1);
    assert_eq!(timers.arm(batch(1), generation(1), deadline(30)), Ok(true));
    assert_eq!(timers.arm(batch(1), generation(1), deadline(5)), Ok(false));
    assert_eq!(timers.arm(batch(1), generation(0), deadline(3)), Ok(false));
    assert_eq!(timers.arm(batch(1), generation(2), deadline(20)), Ok(true));

    assert_eq!(timers.len(), 1);
    assert_eq!(timers.next_deadline(), Some(deadline(20)));
    assert!(timers.drain_due(Moment::from_tick(19)).is_empty());
    let due = timers.drain_due(Moment::from_tick(20));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].batch_id(), batch(1));
    assert_eq!(due[0].generation(), generation(2));
    assert_eq!(due[0].deadline(), deadline(20));
}

#[test]
fn stale_cancellation_does_not_remove_replacement() {
    let mut timers = BatchTimers::new(1);
    assert_eq!(timers.arm(batch(3), generation(1), deadline(10)), Ok(true));
    assert_eq!(timers.arm(batch(3), generation(2), deadline(12)), Ok(true));

    assert!(!timers.cancel(batch(3), generation(1)));
    assert!(!timers.cancel(batch(3), generation(3)));
    assert_eq!(timers.len(), 1);
    assert!(timers.cancel(batch(3), generation(2)));
    assert_eq!(timers.len(), 0);
    assert!(timers.is_empty());
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn equal_deadlines_are_ordered_by_batch_identity() {
    let mut timers = BatchTimers::new(3);
    assert_eq!(timers.arm(batch(9), generation(1), deadline(40)), Ok(true));
    assert_eq!(timers.arm(batch(2), generation(4), deadline(40)), Ok(true));
    assert_eq!(timers.arm(batch(5), generation(7), deadline(40)), Ok(true));

    let due = timers.drain_due(Moment::from_tick(40));
    let batches = due
        .iter()
        .map(|timer| timer.batch_id().get())
        .collect::<Vec<_>>();
    assert_eq!(batches, [2, 5, 9]);
}

#[test]
fn due_timers_are_ordered_before_future_timers() {
    let mut timers = BatchTimers::new(3);
    assert_eq!(timers.arm(batch(8), generation(1), deadline(30)), Ok(true));
    assert_eq!(timers.arm(batch(7), generation(1), deadline(10)), Ok(true));
    assert_eq!(timers.arm(batch(6), generation(1), deadline(20)), Ok(true));

    let due = timers.drain_due(Moment::from_tick(20));
    let ordered = due
        .iter()
        .map(|timer| (timer.deadline().tick(), timer.batch_id().get()))
        .collect::<Vec<_>>();
    assert_eq!(ordered, [(10, 7), (20, 6)]);
    assert_eq!(timers.next_deadline(), Some(deadline(30)));
}

#[test]
fn configured_capacity_rejects_only_new_batches() {
    let mut timers = BatchTimers::new(1);
    assert_eq!(timers.arm(batch(1), generation(1), deadline(30)), Ok(true));
    let at_capacity = timers.arm(batch(2), generation(1), deadline(20));
    assert_eq!(at_capacity, Err(BatchTimerError::capacity(1)));
    assert_eq!(at_capacity.err().map(BatchTimerError::limit), Some(1));
    assert_eq!(timers.arm(batch(1), generation(2), deadline(10)), Ok(true));
    assert_eq!(timers.next_deadline(), Some(deadline(10)));
    assert!(timers.cancel(batch(1), generation(2)));
    assert_eq!(timers.arm(batch(2), generation(1), deadline(20)), Ok(true));
}
