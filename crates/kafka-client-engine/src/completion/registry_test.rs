//! Capacity, abandonment, and bounded publication scenarios.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use super::{
    CompletionRegistry, CompletionRegistryError, ReclaimStatus,
    test_support::{GateWake, finish_reclaims, stop},
};

#[test]
fn bounded_completion_cells_are_allocated_only_before_first_reservation() {
    let Ok(mut registry) = CompletionRegistry::<u8>::new(8_192, 8_192) else {
        panic!("notifier should start");
    };
    assert_eq!(registry.slots.len(), 0);

    let mut reservations = Vec::new();
    for _ in 0..3 {
        let Ok(reservation) = registry.reserve() else {
            panic!("bounded slot should reserve");
        };
        reservations.push(reservation);
    }
    assert_eq!(registry.slots.len(), 3);

    for (id, observer) in reservations {
        assert_eq!(registry.rollback_reservation(id), Ok(()));
        drop(observer);
    }
    let Ok((id, observer)) = registry.reserve() else {
        panic!("vacant allocated slot should be reused");
    };
    assert_eq!(registry.slots.len(), 3);
    assert_eq!(registry.rollback_reservation(id), Ok(()));
    drop(observer);
    stop(&mut registry);
}

#[test]
fn lifecycle_counts_are_exact_without_scanning_fixed_capacity() {
    let Ok(mut registry) = CompletionRegistry::new(8, 8) else {
        panic!("notifier should start");
    };
    assert_eq!(registry.unsettled_len(), 0);
    assert_eq!(registry.published_or_reclaiming_len(), 0);

    let Ok((rolled_back, abandoned)) = registry.reserve() else {
        panic!("rollback slot should reserve");
    };
    assert_eq!(registry.unsettled_len(), 1);
    assert_eq!(registry.rollback_reservation(rolled_back), Ok(()));
    drop(abandoned);
    assert_eq!(registry.unsettled_len(), 0);

    let Ok((id, observer)) = registry.reserve() else {
        panic!("published slot should reserve");
    };
    assert_eq!(registry.unsettled_len(), 1);
    assert_eq!(registry.publish(id, 67), Ok(()));
    assert_eq!(registry.unsettled_len(), 0);
    assert_eq!(registry.published_or_reclaiming_len(), 1);
    assert_eq!(observer.wait(), Ok(67));
    assert_eq!(registry.next_reclaim(), Ok(Some(id)));
    assert_eq!(registry.published_or_reclaiming_len(), 1);
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
    assert_eq!(registry.published_or_reclaiming_len(), 0);
    stop(&mut registry);
}

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
    assert_eq!(join.join_off_notifier(), Ok(()));
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
    assert_eq!(join.join_off_notifier(), Ok(()));
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
    assert_eq!(join.join_off_notifier(), Ok(()));
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

    let Some(cell) = registry.cell_for_test(id) else {
        panic!("completion cell should exist");
    };
    let gate = GateWake::new();
    let lock_gate = Arc::clone(&gate);
    let locker = thread::spawn(move || {
        let _guard = cell.lock_for_test();
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
