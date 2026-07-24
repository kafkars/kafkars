//! Generation-independent receive-ticket publication scenarios.

use std::{sync::Arc, task::Waker};

use crate::completion::test_support::CountingWake;

use super::{AssignedConsumerRecvSignal, AssignedConsumerRecvTicket, AssignedConsumerRecvWait};

#[test]
fn receive_ticket_publishes_the_current_pending_registration() {
    let signal = Arc::new(AssignedConsumerRecvSignal::new());
    let wake = CountingWake::new();
    let waker = Waker::from(Arc::clone(&wake));
    let _registration = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &waker)
        .unwrap_or_else(|error| panic!("arm receive: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerRecvWait::Change));

    AssignedConsumerRecvTicket::new(signal).publish();

    assert!(wake.wait_for_wake().is_some());
}
