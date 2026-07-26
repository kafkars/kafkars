//! Generation-independent event-ticket publication scenarios.

use std::{sync::Arc, task::Waker};

use crate::completion::test_support::CountingWake;

use super::{AssignedConsumerEventSignal, AssignedConsumerEventTicket, AssignedConsumerEventWait};

#[test]
fn event_ticket_publishes_the_current_pending_registration() {
    let signal = Arc::new(AssignedConsumerEventSignal::new());
    let wake = CountingWake::new();
    let waker = Waker::from(Arc::clone(&wake));
    let _registration = signal
        .arm_task(None, AssignedConsumerEventWait::Change, &waker)
        .unwrap_or_else(|error| panic!("arm event: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerEventWait::Change));

    AssignedConsumerEventTicket::new(signal).publish();

    assert!(wake.wait_for_wake().is_some());
}
