//! Generation fencing and notification coalescing scenarios.

use std::sync::Arc;

use crate::completion::test_support::CountingWake;

use super::{AssignedConsumerRecvSignal, AssignedConsumerRecvTicket, AssignedConsumerRecvWait};

#[test]
fn stale_queued_ticket_services_the_current_registration() {
    let signal = Arc::new(AssignedConsumerRecvSignal::new());
    let first_wake = CountingWake::new();
    let first_waker = std::task::Waker::from(Arc::clone(&first_wake));
    let first = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &first_waker)
        .unwrap_or_else(|error| panic!("arm first receive: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerRecvWait::Change));
    let stale_ticket = AssignedConsumerRecvTicket::new(Arc::clone(&signal));
    signal.cancel(first);

    let current_wake = CountingWake::new();
    let current_waker = std::task::Waker::from(Arc::clone(&current_wake));
    let _current = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &current_waker)
        .unwrap_or_else(|error| panic!("arm current receive: {error:?}"));

    assert!(!signal.prepare_notification(AssignedConsumerRecvWait::Change));
    stale_ticket.publish();
    assert_eq!(current_wake.count(), 1);
    assert_eq!(first_wake.count(), 0);
}
