//! Capacity, ordering, cancellation, and owner-preservation deadline scenarios.

use kafka_client_core::{BatchId, Deadline, Moment, OperationId, ProducerInput};

use super::submission_deadline::{
    DueSubmissionDeadline, SubmissionDeadlineError, SubmissionDeadlines,
};

fn batch(value: u64) -> BatchId {
    BatchId::from_raw(value)
}

fn operation(value: u64) -> OperationId {
    OperationId::from_raw(value)
}

fn deadline(value: u64) -> Deadline {
    Deadline::from_tick(value)
}

#[test]
fn due_entries_preserve_owners_and_order_before_future_entries() {
    let mut deadlines = SubmissionDeadlines::new(4);
    assert_eq!(
        deadlines.arm(batch(9), operation(90), deadline(40)),
        Ok(true)
    );
    assert_eq!(
        deadlines.arm(batch(2), operation(20), deadline(40)),
        Ok(true)
    );
    assert_eq!(
        deadlines.arm(batch(7), operation(70), deadline(10)),
        Ok(true)
    );
    assert_eq!(
        deadlines.arm(batch(1), operation(10), deadline(60)),
        Ok(true)
    );

    let due: Vec<DueSubmissionDeadline> = deadlines.drain_due(Moment::from_tick(40), usize::MAX);
    let facts = due
        .iter()
        .map(|entry| {
            (
                entry.deadline().tick(),
                entry.batch_id().get(),
                entry.operation_id().get(),
                entry.observed_at().tick(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(facts, [(10, 7, 70, 40), (40, 2, 20, 40), (40, 9, 90, 40)]);
    assert_eq!(deadlines.len(), 1);
    assert_eq!(deadlines.next_deadline(), Some(deadline(60)));
}

#[test]
fn capacity_rejects_new_batches_but_exact_replay_is_idempotent() {
    let mut deadlines = SubmissionDeadlines::new(1);
    assert_eq!(
        deadlines.arm(batch(1), operation(10), deadline(30)),
        Ok(true)
    );
    assert_eq!(
        deadlines.arm(batch(1), operation(10), deadline(30)),
        Ok(false)
    );
    assert_eq!(
        deadlines.arm(batch(2), operation(20), deadline(20)),
        Err(SubmissionDeadlineError::Capacity { limit: 1 })
    );
    assert_eq!(deadlines.len(), 1);
    assert_eq!(deadlines.next_deadline(), Some(deadline(30)));
}

#[test]
fn conflicting_duplicate_never_replaces_core_declared_facts() {
    let mut deadlines = SubmissionDeadlines::new(1);
    assert_eq!(
        deadlines.arm(batch(1), operation(10), deadline(30)),
        Ok(true)
    );
    for (owner, due_at) in [(11, 30), (10, 31)] {
        assert_eq!(
            deadlines.arm(batch(1), operation(owner), deadline(due_at)),
            Err(SubmissionDeadlineError::ConflictingBatch { batch_id: batch(1) })
        );
    }

    let due = deadlines.drain_due(Moment::from_tick(30), usize::MAX);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].operation_id(), operation(10));
    assert_eq!(due[0].deadline(), deadline(30));
}

#[test]
fn cancellation_is_exact_before_or_after_deadline_transfer() {
    let mut deadlines = SubmissionDeadlines::new(2);
    assert_eq!(
        deadlines.arm(batch(1), operation(10), deadline(20)),
        Ok(true)
    );
    assert_eq!(
        deadlines.arm(batch(2), operation(20), deadline(30)),
        Ok(true)
    );

    assert!(deadlines.cancel(batch(1)));
    assert!(!deadlines.cancel(batch(1)));
    assert!(deadlines.drain_due(Moment::from_tick(20), 1).is_empty());
    assert_eq!(deadlines.drain_due(Moment::from_tick(30), 1).len(), 1);
    assert!(!deadlines.cancel(batch(2)));
    assert!(deadlines.is_empty());
}

#[test]
fn due_entry_constructs_the_exact_core_deadline_fact() {
    let mut deadlines = SubmissionDeadlines::new(1);
    assert_eq!(
        deadlines.arm(batch(4), operation(44), deadline(12)),
        Ok(true)
    );
    let mut due = deadlines.drain_due(Moment::from_tick(15), 1);
    let fact = due
        .pop()
        .unwrap_or_else(|| panic!("deadline should be due"));

    assert_eq!(
        fact.into_input(),
        ProducerInput::DeadlineElapsed {
            operation_id: operation(44),
            now: Moment::from_tick(15),
        }
    );
}

#[test]
fn bounded_drain_leaves_equal_deadlines_ready_in_batch_order() {
    let mut deadlines = SubmissionDeadlines::new(3);
    for value in [3, 1, 2] {
        assert_eq!(
            deadlines.arm(batch(value), operation(value * 10), deadline(15)),
            Ok(true)
        );
    }

    let first = deadlines.drain_due(Moment::from_tick(15), 2);
    let ids = first
        .iter()
        .map(|entry| entry.batch_id().get())
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 2]);
    assert_eq!(deadlines.len(), 1);
    assert_eq!(deadlines.next_deadline(), Some(deadline(15)));
    assert_eq!(deadlines.drain_due(Moment::from_tick(15), 1).len(), 1);
}
