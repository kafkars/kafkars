//! Event generation fencing and notification coalescing scenarios.

use std::sync::Arc;

use crate::completion::test_support::CountingWake;

use super::{AssignedConsumerEventSignal, AssignedConsumerEventTicket, AssignedConsumerEventWait};

#[test]
fn stale_queued_ticket_services_the_current_event_registration() {
    let signal = Arc::new(AssignedConsumerEventSignal::new());
    let first_wake = CountingWake::new();
    let first_waker = std::task::Waker::from(Arc::clone(&first_wake));
    let first = signal
        .arm_task(None, AssignedConsumerEventWait::Change, &first_waker)
        .unwrap_or_else(|error| panic!("arm first event: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerEventWait::Change));
    let stale_ticket = AssignedConsumerEventTicket::new(Arc::clone(&signal));
    signal.cancel(first);

    let current_wake = CountingWake::new();
    let current_waker = std::task::Waker::from(Arc::clone(&current_wake));
    let _current = signal
        .arm_task(None, AssignedConsumerEventWait::Change, &current_waker)
        .unwrap_or_else(|error| panic!("arm current event: {error:?}"));

    assert!(!signal.prepare_notification(AssignedConsumerEventWait::Change));
    stale_ticket.publish();
    assert_eq!(current_wake.count(), 1);
    assert_eq!(first_wake.count(), 0);
}
