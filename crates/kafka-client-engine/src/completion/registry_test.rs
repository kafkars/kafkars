//! Capacity, abandonment, and bounded publication scenarios.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    thread,
};

use super::{
    CompletionRegistry, CompletionRegistryError, ReclaimStatus,
    test_support::{CountingWake, GateWake, finish_reclaims, poll_once, stop},
};

#[test]
fn unobserved_terminal_retains_fixed_capacity() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert_eq!(registry.publish(id, 71), Ok(()));
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));

    drop(observer);
    let Ok(join) = registry.stop_notifier() else {
        panic!("notifier should stop");
    };
    assert_eq!(join.join(), Ok(()));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::NotifierStopped)
    ));
}

#[test]
fn pending_abandonment_settles_and_reclaims_exactly_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    drop(observer);
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    assert_eq!(registry.next_reclaim(), Ok(None));

    let terminal = DropProbe(Arc::clone(&drops));
    assert!(registry.publish(id, terminal).is_ok());
    let Ok(join) = registry.stop_notifier() else {
        panic!("notifier should stop");
    };
    assert_eq!(join.join(), Ok(()));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    assert_eq!(finish_reclaims(&mut registry), Ok(0));
}

#[test]
fn abandoned_value_is_dropped_before_reclaim_becomes_visible() {
    let gate = GateWake::new();
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    drop(observer);
    assert!(
        registry
            .publish(id, BlockingDrop(Arc::clone(&gate)))
            .is_ok()
    );
    assert!(gate.wait_until_entered());
    assert_eq!(registry.next_reclaim(), Ok(None));

    let Ok(join) = registry.stop_notifier() else {
        panic!("notifier stop should not wait");
    };
    gate.release();
    assert_eq!(join.join(), Ok(()));
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
}

#[test]
fn duplicate_publish_never_replaces_the_first_terminal() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert_eq!(registry.publish(id, 73), Ok(()));
    assert_eq!(
        registry.publish(id, 79),
        Err((CompletionRegistryError::DuplicatePublish, 79))
    );
    assert_eq!(observer.wait(), Ok(73));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn notifier_shutdown_refuses_an_unsettled_reservation() {
    let Ok(mut registry) = CompletionRegistry::<u8>::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert!(matches!(
        registry.stop_notifier(),
        Err(CompletionRegistryError::UnsettledCompletion)
    ));
    assert_eq!(registry.publish(id, 1), Ok(()));
    assert_eq!(observer.wait(), Ok(1));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn rejected_core_admission_rolls_engine_reservation_back() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((rejected_id, rejected_observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert_eq!(registry.rollback_reservation(rejected_id), Ok(()));
    drop(rejected_observer);

    let Ok((accepted_id, accepted_observer)) = registry.reserve() else {
        panic!("rolled-back slot should reserve");
    };
    assert_ne!(accepted_id, rejected_id);
    assert_eq!(registry.publish(accepted_id, 7), Ok(()));
    assert_eq!(accepted_observer.wait(), Ok(7));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

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

#[test]
fn core_handshake_precedes_engine_slot_reuse() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert_eq!(registry.publish(id, 101), Ok(()));
    assert_eq!(observer.wait(), Ok(101));
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));
    assert!(matches!(
        registry.reserve(),
        Err(CompletionRegistryError::Full)
    ));
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
    let Ok((next_id, next_observer)) = registry.reserve() else {
        panic!("reclaimed slot should reserve");
    };
    assert_eq!(registry.publish(next_id, 103), Ok(()));
    assert_eq!(next_observer.wait(), Ok(103));
    assert_eq!(finish_reclaims(&mut registry), Ok(1));
    stop(&mut registry);
}

#[test]
fn reclaim_retries_without_blocking_while_observer_state_is_locked() {
    let Ok(mut registry) = CompletionRegistry::new(1, 1) else {
        panic!("notifier should start");
    };
    let Ok((id, observer)) = registry.reserve() else {
        panic!("slot should reserve");
    };
    assert_eq!(registry.publish(id, 107), Ok(()));
    assert_eq!(observer.wait(), Ok(107));
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));

    let cell = Arc::clone(&registry.slots[id.slot()].cell);
    let gate = GateWake::new();
    let lock_gate = Arc::clone(&gate);
    let locker = thread::spawn(move || {
        let _guard = cell.lock();
        lock_gate.block_until_released();
    });
    assert!(gate.wait_until_entered());
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Retry));

    gate.release();
    assert!(locker.join().is_ok());
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
    stop(&mut registry);
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

struct BlockingDrop(Arc<GateWake>);

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        self.0.block_until_released();
    }
}
