//! Assigned-consumer notifier ownership and shutdown scenarios.

use std::sync::Arc;

use crate::completion::{
    CompletionRegistry, CompletionRegistryError,
    test_support::{CountingWake, GateWake, poll_once},
};

use super::{
    close_observer::AssignedConsumerCloseTerminal,
    completion::AssignedConsumerCompletionNotifier,
    next_event::{
        AssignedConsumerEventSignal, AssignedConsumerEventTicket, AssignedConsumerEventWait,
    },
    recv::{AssignedConsumerRecvSignal, AssignedConsumerRecvTicket, AssignedConsumerRecvWait},
};

#[test]
fn one_consumer_notifier_issues_three_typed_ports_and_stops_linearly() {
    let (mut notifier, _ports) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));

    assert!(notifier.thread_id().is_some());
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert!(notifier.thread_id().is_none());
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

#[test]
fn notifier_capacity_is_exactly_close_recv_and_event_capacity() {
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut closes = CompletionRegistry::with_publisher(3, publishers.close);
    let (gate_id, mut gate_observer) = closes
        .reserve()
        .unwrap_or_else(|error| panic!("reserve gate close: {error}"));
    let (queued_id, queued_observer) = closes
        .reserve()
        .unwrap_or_else(|error| panic!("reserve queued close: {error}"));
    let (overflow_id, overflow_observer) = closes
        .reserve()
        .unwrap_or_else(|error| panic!("reserve overflow close: {error}"));
    let gate = GateWake::new();
    assert_eq!(
        poll_once(&mut gate_observer, Arc::clone(&gate)),
        std::task::Poll::Pending
    );
    closes
        .publish(gate_id, AssignedConsumerCloseTerminal::ExecutionUnavailable)
        .unwrap_or_else(|(error, _terminal)| panic!("publish gate close: {error}"));
    assert!(gate.wait_until_entered());

    closes
        .publish(
            queued_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable,
        )
        .unwrap_or_else(|(error, _terminal)| panic!("queue close: {error}"));
    let recv = armed_recv();
    let event = armed_event();
    publishers
        .recv
        .try_publish(AssignedConsumerRecvTicket::new(Arc::clone(&recv.0)))
        .unwrap_or_else(|_ticket| panic!("queue receive notification"));
    publishers
        .event
        .try_publish(AssignedConsumerEventTicket::new(Arc::clone(&event.0)))
        .unwrap_or_else(|_ticket| panic!("queue event notification"));

    assert!(matches!(
        closes.publish(
            overflow_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable
        ),
        Err((
            CompletionRegistryError::NotificationBackpressure,
            AssignedConsumerCloseTerminal::ExecutionUnavailable
        ))
    ));

    gate.release();
    assert!(recv.1.wait_for_wake().is_some());
    assert!(event.1.wait_for_wake().is_some());
    assert!(gate_observer.wait().is_ok());
    assert!(queued_observer.wait().is_ok());
    drop(overflow_observer);
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

#[test]
fn stale_recv_ticket_in_a_full_queue_wakes_the_current_generation() {
    let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut closes = CompletionRegistry::with_publisher(2, publishers.close);
    let (gate_id, mut gate_observer) = closes
        .reserve()
        .unwrap_or_else(|error| panic!("reserve gate close: {error}"));
    let (queued_id, queued_observer) = closes
        .reserve()
        .unwrap_or_else(|error| panic!("reserve queued close: {error}"));
    let gate = GateWake::new();
    assert_eq!(
        poll_once(&mut gate_observer, Arc::clone(&gate)),
        std::task::Poll::Pending
    );
    closes
        .publish(gate_id, AssignedConsumerCloseTerminal::ExecutionUnavailable)
        .unwrap_or_else(|(error, _terminal)| panic!("publish gate close: {error}"));
    assert!(gate.wait_until_entered());

    let signal = Arc::new(AssignedConsumerRecvSignal::new());
    let old_wake = CountingWake::new();
    let old_waker = std::task::Waker::from(Arc::clone(&old_wake));
    let old = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &old_waker)
        .unwrap_or_else(|error| panic!("arm old receive: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerRecvWait::Change));
    publishers
        .recv
        .try_publish(AssignedConsumerRecvTicket::new(Arc::clone(&signal)))
        .unwrap_or_else(|_ticket| panic!("queue stale receive ticket"));
    signal.cancel(old);

    let current_wake = CountingWake::new();
    let current_waker = std::task::Waker::from(Arc::clone(&current_wake));
    let _current = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &current_waker)
        .unwrap_or_else(|error| panic!("arm current receive: {error:?}"));
    closes
        .publish(
            queued_id,
            AssignedConsumerCloseTerminal::ExecutionUnavailable,
        )
        .unwrap_or_else(|(error, _terminal)| panic!("fill queue with close: {error}"));
    assert!(!signal.prepare_notification(AssignedConsumerRecvWait::Change));

    gate.release();
    assert!(current_wake.wait_for_wake().is_some());
    assert_eq!(old_wake.count(), 0);
    assert!(gate_observer.wait().is_ok());
    assert!(queued_observer.wait().is_ok());
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}

fn armed_recv() -> (Arc<AssignedConsumerRecvSignal>, Arc<CountingWake>) {
    let signal = Arc::new(AssignedConsumerRecvSignal::new());
    let wake = CountingWake::new();
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let _registration = signal
        .arm_task(None, AssignedConsumerRecvWait::Change, &waker)
        .unwrap_or_else(|error| panic!("arm receive: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerRecvWait::Change));
    (signal, wake)
}

fn armed_event() -> (Arc<AssignedConsumerEventSignal>, Arc<CountingWake>) {
    let signal = Arc::new(AssignedConsumerEventSignal::new());
    let wake = CountingWake::new();
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let _registration = signal
        .arm_task(None, AssignedConsumerEventWait::Change, &waker)
        .unwrap_or_else(|error| panic!("arm event: {error:?}"));
    assert!(signal.prepare_notification(AssignedConsumerEventWait::Change));
    (signal, wake)
}
