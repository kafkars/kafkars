//! Scenarios for one shared async and blocking completion cell.

use std::{
    sync::{Arc, Barrier},
    task::Poll,
    thread,
};

use super::{
    CompletionRegistry,
    test_support::{CountingWake, PanicWake, finish_reclaims, poll_once, stop},
};

#[test]
fn publish_before_poll_reads_the_stored_terminal() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, mut observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };

    assert_eq!(registry.publish(id, 17), Ok(()));
    let wake = CountingWake::new();
    match poll_once(&mut observer, Arc::clone(&wake)) {
        Poll::Ready(result) => assert_eq!(result, Ok(17)),
        Poll::Pending => {
            assert!(wake.wait_for_wake().is_some());
            assert_eq!(poll_once(&mut observer, wake), Poll::Ready(Ok(17)));
        }
    }
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn poll_before_publish_wakes_only_on_the_notifier_thread() {
    let publishing_thread = thread::current().id();
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, mut observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    let wake = CountingWake::new();

    assert_eq!(poll_once(&mut observer, Arc::clone(&wake)), Poll::Pending);
    assert_eq!(registry.publish(id, 29), Ok(()));
    let Some(notifier_thread) = wake.wait_for_wake() else {
        panic!("notifier should wake observer");
    };
    assert_ne!(notifier_thread, publishing_thread);
    assert_eq!(poll_once(&mut observer, wake), Poll::Ready(Ok(29)));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn later_poll_replaces_an_obsolete_waker() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, mut observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    let first = CountingWake::new();
    let second = CountingWake::new();

    assert_eq!(poll_once(&mut observer, Arc::clone(&first)), Poll::Pending);
    assert_eq!(poll_once(&mut observer, Arc::clone(&second)), Poll::Pending);
    assert_eq!(registry.publish(id, 41), Ok(()));
    assert!(second.wait_for_wake().is_some());
    assert_eq!(first.count(), 0);
    assert_eq!(poll_once(&mut observer, second), Poll::Ready(Ok(41)));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn blocking_wait_uses_the_same_cell_before_and_after_publish() {
    let Ok(mut registry) = CompletionRegistry::new(2, 2) else {
        panic!("notifier should start");
    };
    let Ok((first_id, first)) = registry.reserve() else {
        panic!("first slot should reserve");
    };
    let waiter = thread::spawn(move || first.wait());
    assert_eq!(registry.publish(first_id, 53), Ok(()));
    let Ok(first_result) = waiter.join() else {
        panic!("waiter should not panic");
    };
    assert_eq!(first_result, Ok(53));

    let Ok((second_id, second)) = registry.reserve() else {
        panic!("second slot should reserve");
    };
    assert_eq!(registry.publish(second_id, 59), Ok(()));
    assert_eq!(second.wait(), Ok(59));
    assert_eq!(finish_reclaims(&mut registry), Ok(2));
    stop(&mut registry);
}

#[test]
fn a_panicking_waker_does_not_kill_the_notifier() {
    let Ok(mut registry) = CompletionRegistry::new(2, 2) else {
        panic!("notifier should start");
    };
    let Ok((panic_id, mut panic_observer)) = registry.reserve() else {
        panic!("first slot should reserve");
    };
    assert_eq!(
        poll_once(&mut panic_observer, Arc::new(PanicWake)),
        Poll::Pending
    );
    assert_eq!(registry.publish(panic_id, 61), Ok(()));

    let Ok((live_id, mut live_observer)) = registry.reserve() else {
        panic!("second slot should reserve");
    };
    let live = CountingWake::new();
    assert_eq!(
        poll_once(&mut live_observer, Arc::clone(&live)),
        Poll::Pending
    );
    assert_eq!(registry.publish(live_id, 67), Ok(()));
    assert!(live.wait_for_wake().is_some());
    assert_eq!(poll_once(&mut live_observer, live), Poll::Ready(Ok(67)));
    drop(panic_observer);
    assert_eq!(finish_reclaims(&mut registry), Ok(2));
    stop(&mut registry);
}

#[test]
fn concurrent_poll_and_publication_never_lose_a_wakeup() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    for value in 0..32 {
        let Ok((id, mut observer)) = registry.reserve() else {
            panic!("slot should reserve");
        };
        let barrier = Arc::new(Barrier::new(2));
        let wake = CountingWake::new();
        let poll_barrier = Arc::clone(&barrier);
        let poll_wake = Arc::clone(&wake);
        let poller = thread::spawn(move || {
            poll_barrier.wait();
            match poll_once(&mut observer, Arc::clone(&poll_wake)) {
                Poll::Ready(result) => result,
                Poll::Pending => {
                    let Some(_) = poll_wake.wait_for_wake() else {
                        panic!("publication should wake the pending observer")
                    };
                    match poll_once(&mut observer, poll_wake) {
                        Poll::Ready(result) => result,
                        Poll::Pending => panic!("woken observer should be terminal"),
                    }
                }
            }
        });
        barrier.wait();
        assert_eq!(registry.publish(id, value), Ok(()));
        let Ok(result) = poller.join() else {
            panic!("poller should not panic");
        };
        assert_eq!(result, Ok(value));
        assert_eq!(finish_reclaims(&mut registry), Ok(1));
    }
    stop(&mut registry);
}

#[test]
fn panicking_abandoned_terminal_drop_does_not_kill_the_notifier() {
    let Ok(mut registry) = CompletionRegistry::new(2, 2) else {
        panic!("notifier should start");
    };
    let Ok((abandoned_id, abandoned)) = registry.reserve() else {
        panic!("first slot should reserve");
    };
    drop(abandoned);
    assert_eq!(registry.publish(abandoned_id, MaybePanicDrop(true)), Ok(()));

    let Ok((live_id, live)) = registry.reserve() else {
        panic!("second slot should reserve");
    };
    assert_eq!(registry.publish(live_id, MaybePanicDrop(false)), Ok(()));
    let Ok(result) = live.wait() else {
        panic!("live terminal should remain observable");
    };
    assert!(!result.0);
    assert_eq!(finish_reclaims(&mut registry), Ok(2));
    stop(&mut registry);
}

#[derive(Debug, Eq, PartialEq)]
struct MaybePanicDrop(bool);

impl Drop for MaybePanicDrop {
    fn drop(&mut self) {
        assert!(!self.0, "intentional terminal drop panic");
    }
}
