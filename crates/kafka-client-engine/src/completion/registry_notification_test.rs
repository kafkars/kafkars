//! Bounded notifier sizing and queued-publication scenarios.

use std::{sync::Arc, task::Poll};

use super::{
    CompletionRegistry,
    test_support::{CountingWake, GateWake, finish_reclaims, poll_once, stop},
};

#[test]
fn notification_capacity_must_cover_every_terminal_slot() {
    let error = CompletionRegistry::<u8>::new(3, 2)
        .err()
        .unwrap_or_else(|| panic!("undersized notifier queue should be rejected"));
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn bounded_notifier_queue_accepts_every_reserved_terminal() {
    let Ok(mut registry) = CompletionRegistry::new(3, 3) else {
        panic!("notifier should start");
    };
    let Ok((gate_id, mut gate_observer)) = registry.reserve() else {
        panic!("gate slot should reserve");
    };
    let gate = GateWake::new();
    assert_eq!(
        poll_once(&mut gate_observer, Arc::clone(&gate)),
        Poll::Pending
    );
    assert_eq!(registry.publish(gate_id, 83), Ok(()));
    assert!(gate.wait_until_entered());

    let Ok((queued_id, mut queued_observer)) = registry.reserve() else {
        panic!("queued slot should reserve");
    };
    let queued = CountingWake::new();
    assert_eq!(
        poll_once(&mut queued_observer, Arc::clone(&queued)),
        Poll::Pending
    );
    assert_eq!(registry.publish(queued_id, 89), Ok(()));

    let Ok((pending_id, mut pending_observer)) = registry.reserve() else {
        panic!("pending slot should reserve");
    };
    let pending = CountingWake::new();
    assert_eq!(
        poll_once(&mut pending_observer, Arc::clone(&pending)),
        Poll::Pending
    );
    assert_eq!(registry.publish(pending_id, 97), Ok(()));

    gate.release();
    assert!(queued.wait_for_wake().is_some());
    assert!(pending.wait_for_wake().is_some());
    assert_eq!(poll_once(&mut gate_observer, gate), Poll::Ready(Ok(83)));
    assert_eq!(poll_once(&mut queued_observer, queued), Poll::Ready(Ok(89)));
    assert_eq!(
        poll_once(&mut pending_observer, pending),
        Poll::Ready(Ok(97))
    );
    assert_eq!(finish_reclaims(&mut registry), Ok(3));
    stop(&mut registry);
}
