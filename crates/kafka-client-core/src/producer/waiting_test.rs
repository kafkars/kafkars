//! Deterministic tests for bounded FIFO producer waiting policy.

use crate::{
    ByteCount, Deadline, Moment, ProducerWaiter, ProducerWaitingAdmissionError,
    ProducerWaitingQueue,
};

#[test]
fn fifo_owner_accounts_independent_count_bytes_and_deadlines() {
    let mut queue = ProducerWaitingQueue::new(2, ByteCount::new(7));
    let Ok(first) = queue.admit(
        Moment::from_tick(1),
        Deadline::from_tick(9),
        ByteCount::new(3),
    ) else {
        panic!("first waiter");
    };
    let Ok(second) = queue.admit(
        Moment::from_tick(1),
        Deadline::from_tick(7),
        ByteCount::new(4),
    ) else {
        panic!("second waiter");
    };

    assert_eq!(queue.front().map(ProducerWaiter::id), Some(first));
    assert_eq!(queue.next_deadline(), Some(Deadline::from_tick(7)));
    assert_eq!(queue.retained_bytes(), ByteCount::new(7));
    assert_eq!(
        queue.admit(
            Moment::from_tick(1),
            Deadline::from_tick(10),
            ByteCount::new(1)
        ),
        Err(ProducerWaitingAdmissionError::RecordCapacity)
    );
    assert_eq!(queue.remove(first).map(ProducerWaiter::id), Some(first));
    assert_eq!(queue.front().map(ProducerWaiter::id), Some(second));
    assert_eq!(queue.retained_bytes(), ByteCount::new(4));
}

#[test]
fn timeout_close_and_byte_capacity_are_explicit() {
    let mut queue = ProducerWaitingQueue::new(2, ByteCount::new(4));
    assert_eq!(
        queue.admit(
            Moment::from_tick(5),
            Deadline::from_tick(5),
            ByteCount::new(1)
        ),
        Err(ProducerWaitingAdmissionError::DeadlineElapsed)
    );
    let Ok(_waiter) = queue.admit(
        Moment::from_tick(5),
        Deadline::from_tick(6),
        ByteCount::new(4),
    ) else {
        panic!("bounded waiter");
    };
    assert_eq!(
        queue.admit(
            Moment::from_tick(5),
            Deadline::from_tick(8),
            ByteCount::new(1)
        ),
        Err(ProducerWaitingAdmissionError::ByteCapacity)
    );
    assert!(queue.first_elapsed(Moment::from_tick(6)).is_some());
    queue.close();
    assert_eq!(
        queue.admit(
            Moment::from_tick(6),
            Deadline::from_tick(9),
            ByteCount::new(0)
        ),
        Err(ProducerWaitingAdmissionError::Closed)
    );
}
