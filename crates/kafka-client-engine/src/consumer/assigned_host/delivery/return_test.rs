//! Exact Drop reclamation, close blocking, and owner-lock ordering.

use std::sync::{
    Arc, Barrier, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
    mpsc::sync_channel,
};
use std::{thread, time::Duration};

use crate::consumer::assigned_host::{
    self, AssignedConsumerAssignment, AssignedConsumerStartPosition,
    claim::AssignedConsumerClaimSlot,
    shard::{AssignedConsumerPort, AssignedConsumerShardLockError, AssignedConsumerShardOwner},
    shard_test,
    state::AssignedConsumerShardState,
    wake::{AssignedConsumerShardWake, AssignedConsumerShardWakeError},
};
use crate::{
    clock::MonotonicClock,
    consumer::{
        assigned_owner_close_test::install_pending_ready,
        assigned_owner_effect::FrontEffect,
        assigned_owner_test::{limits, settings},
    },
};

#[test]
fn batch_drop_reclaims_exact_bytes_before_close_can_complete() {
    let (owner, port, _wake) = shard_test::setup();
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));
    let _close = handle
        .try_close()
        .unwrap_or_else(|error| panic!("accept close: {error}"));
    owner
        .try_with_owner(|assigned| {
            drain_effects(assigned);
            assert!(!assigned.progress_close());
            assert_eq!(assigned.fetches.retained().1, 1);
        })
        .unwrap_or_else(|error| panic!("inspect leased close: {error:?}"));

    drop(batch);

    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.fetches.retained(), (0, 0, 0));
            assert!(assigned.progress_close());
        })
        .unwrap_or_else(|error| panic!("inspect reclaimed close: {error:?}"));
}

#[test]
fn batch_drop_releases_owner_lock_before_requesting_reactor_work() {
    let wake = Arc::new(LockInspectingWake::default());
    let (owner, port) = AssignedConsumerShardOwner::new_for_test(
        Arc::new(MonotonicClock::new()),
        settings(),
        limits(2),
        Arc::clone(&wake),
    )
    .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));
    wake.install(&port);
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);
    wake.observed_unlocked.store(false, Ordering::Release);
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));

    drop(batch);

    assert!(wake.observed_unlocked.load(Ordering::Acquire));
}

#[test]
fn contended_batch_drop_waits_then_reclaims_exact_count_and_bytes() {
    let (owner, port, _wake) = shard_test::setup();
    let shared = Arc::clone(&port.shared);
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));
    let retained = owner
        .inspect_terminal(|assigned| assigned.fetches.retained())
        .unwrap_or_else(|error| panic!("inspect retained delivery: {error:?}"));
    assert_eq!(retained.1, 1);
    assert_ne!(retained.2, 0);

    let release = Arc::new(Barrier::new(2));
    let (locked_tx, locked_rx) = sync_channel(0);
    let lock_release = Arc::clone(&release);
    let lock_thread = thread::spawn(move || {
        let guard = shared.lock_for_test();
        locked_tx
            .send(())
            .unwrap_or_else(|error| panic!("publish held lock: {error}"));
        lock_release.wait();
        drop(guard);
    });
    locked_rx
        .recv()
        .unwrap_or_else(|error| panic!("wait for held lock: {error}"));

    let (dropped_tx, dropped_rx) = sync_channel(0);
    let drop_thread = thread::spawn(move || {
        drop(batch);
        dropped_tx
            .send(())
            .unwrap_or_else(|error| panic!("publish batch drop: {error}"));
    });
    assert!(dropped_rx.recv_timeout(Duration::from_millis(25)).is_err());

    release.wait();
    lock_thread
        .join()
        .unwrap_or_else(|_panic| panic!("owner-lock thread panicked"));
    dropped_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("batch Drop did not finish: {error}"));
    drop_thread
        .join()
        .unwrap_or_else(|_panic| panic!("batch-drop thread panicked"));
    assert_eq!(
        owner
            .inspect_terminal(|assigned| assigned.fetches.retained())
            .unwrap_or_else(|error| panic!("inspect reclaimed delivery: {error:?}")),
        (0, 0, 0)
    );
}

#[test]
fn failed_drop_reclaim_retains_the_exact_lease_in_owner_fault_state() {
    let (owner, port, _wake) = shard_test::setup();
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));
    owner
        .try_with_owner(|assigned| assigned.fetches.install_fault_for_test())
        .unwrap_or_else(|error| panic!("install Fetch fault: {error:?}"));

    drop(batch);

    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.reclaim_faults.len(), 1);
            assert_eq!(assigned.fetches.retained().1, 1);
        })
        .unwrap_or_else(|error| panic!("inspect reclaim fault: {error:?}"));
}

#[test]
fn owner_missing_drop_occurs_only_after_recovery_released_store_accounting() {
    let (owner, port, wake) = shard_test::setup();
    let mut handle = claim(port);
    assign_and_prepare(&owner, &mut handle);
    let batch = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take batch: {error}"))
        .unwrap_or_else(|| panic!("ready batch"));
    assert_eq!(
        owner
            .inspect_terminal(|assigned| assigned.fetches.retained().1)
            .unwrap_or_else(|error| panic!("inspect delivery lease: {error:?}")),
        1
    );
    let wake_count = wake.count();

    let recovery = owner
        .take_assigned_owner_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover owner after driver shutdown: {error:?}"));
    assert!(recovery.requires_cleanup_report());
    assert!(matches!(
        owner.inspect_terminal(|_assigned| ()),
        Err(AssignedConsumerShardLockError::OwnerMissing)
    ));

    drop(batch);

    assert_eq!(wake.count(), wake_count);
}

fn claim(port: AssignedConsumerPort) -> assigned_host::AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assign_and_prepare(
    owner: &AssignedConsumerShardOwner,
    handle: &mut assigned_host::AssignedConsumerHandle,
) {
    let entry =
        AssignedConsumerAssignment::try_new("orders", 0, AssignedConsumerStartPosition::Offset(10))
            .unwrap_or_else(|error| panic!("assignment entry: {error}"));
    let _accepted = handle
        .try_replace_assignment(vec![entry], Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("replace assignment: {error}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready(assigned, 10);
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
}

fn drain_effects(owner: &mut crate::consumer::AssignedConsumerOwner) {
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
}

#[derive(Default)]
struct LockInspectingWake {
    state: Mutex<Option<Weak<AssignedConsumerShardState>>>,
    observed_unlocked: AtomicBool,
}

impl LockInspectingWake {
    fn install(&self, port: &AssignedConsumerPort) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = Some(Arc::downgrade(&port.shared));
    }
}

impl AssignedConsumerShardWake for LockInspectingWake {
    fn request_assigned_turn(&self) -> Result<(), AssignedConsumerShardWakeError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(state) = state {
            self.observed_unlocked
                .store(state.try_with_owner(|_owner| ()).is_ok(), Ordering::Release);
        }
        Ok(())
    }
}
