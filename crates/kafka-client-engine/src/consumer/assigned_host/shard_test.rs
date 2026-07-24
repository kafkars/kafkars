//! Unique assigned-shard ownership, contention, and shutdown recovery.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::sync_channel,
    },
    thread,
    time::Duration,
};

use super::super::{
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_test::{driver, limits, settings, shutdown},
};
use super::{
    result::AssignedConsumerPortError,
    shard::{AssignedConsumerPort, AssignedConsumerShardLockError, AssignedConsumerShardOwner},
    wake::{AssignedConsumerShardWake, AssignedConsumerShardWakeError},
};
use crate::clock::MonotonicClock;

#[test]
fn one_nonclone_port_shares_the_unique_optional_owner_slot() {
    let (owner, _port, _wake) = setup();
    assert!(owner.try_with_owner(|owner| owner.unsettled()).is_ok());

    let guard = owner.lock_for_test();
    assert_eq!(
        owner.try_with_owner(|owner| owner.unsettled()),
        Err(AssignedConsumerShardLockError::Contended)
    );
    drop(guard);
}

#[test]
fn abnormal_recovery_consumes_the_owner_only_after_driver_shutdown() {
    let (owner, port, _wake) = setup();
    owner
        .try_with_owner(|assigned| {
            assigned.fault = Some(AssignedConsumerOwnerFault::Clock(
                crate::clock::ClockError::TickOverflow,
            ));
        })
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));
    let mut driver = driver();
    shutdown(&mut driver);

    let recovery = owner
        .take_assigned_owner_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovery owner: {error:?}"));

    assert!(recovery.requires_cleanup_report());
    assert_eq!(
        owner.try_with_owner(|assigned| assigned.unsettled()),
        Err(AssignedConsumerShardLockError::OwnerMissing)
    );
    assert!(matches!(
        port.begin_close(),
        Err(AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::OwnerMissing
        ))
    ));
}

#[test]
fn host_owner_drop_leaves_core_completed_close_for_the_unique_port() {
    let (owner, port, _wake) = setup();
    let accepted = port
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    let mut driver = driver();
    for _attempt in 0..8 {
        let completed = owner
            .try_with_owner(|assigned| {
                let _turn = assigned.turn(&driver);
                assigned.close_completed() && assigned.unsettled() == 0
            })
            .unwrap_or_else(|error| panic!("owner turn: {error:?}"));
        if completed {
            break;
        }
    }
    assert!(
        owner
            .try_with_owner(|assigned| { assigned.close_completed() && assigned.unsettled() == 0 })
            .unwrap_or_else(|error| panic!("owner slot: {error:?}"))
    );
    shutdown(&mut driver);
    drop(owner);

    assert_eq!(accepted.into_value().wait(), Ok(()));
}

#[test]
fn closing_admission_waits_out_the_active_owner_critical_section() {
    let (owner, _port, _wake) = setup();
    let guard = owner.lock_for_test();
    let (started_sender, started_receiver) = sync_channel::<()>(0);
    let (closed_sender, closed_receiver) = sync_channel::<()>(0);
    thread::scope(|scope| {
        scope.spawn(|| {
            started_sender
                .send(())
                .unwrap_or_else(|error| panic!("start close: {error}"));
            owner
                .close_assigned_admission()
                .unwrap_or_else(|error| panic!("close admission: {error:?}"));
            closed_sender
                .send(())
                .unwrap_or_else(|error| panic!("finish close: {error}"));
        });
        started_receiver
            .recv()
            .unwrap_or_else(|error| panic!("observe close: {error}"));
        assert!(
            closed_receiver
                .recv_timeout(Duration::from_millis(10))
                .is_err()
        );
        drop(guard);
        closed_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("synchronized close: {error}"));
    });
}

pub(super) fn setup() -> (
    AssignedConsumerShardOwner,
    AssignedConsumerPort,
    Arc<CountingWake>,
) {
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(CountingWake::default());
    let (owner, port) =
        AssignedConsumerShardOwner::new_for_test(clock, settings(), limits(2), Arc::clone(&wake))
            .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));
    (owner, port, wake)
}

#[derive(Default)]
pub(super) struct CountingWake {
    count: AtomicUsize,
}

impl CountingWake {
    pub(super) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

impl AssignedConsumerShardWake for CountingWake {
    fn request_assigned_turn(&self) -> Result<(), AssignedConsumerShardWakeError> {
        self.count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

pub(super) struct FailingWake;

impl AssignedConsumerShardWake for FailingWake {
    fn request_assigned_turn(&self) -> Result<(), AssignedConsumerShardWakeError> {
        Err(AssignedConsumerShardWakeError::from_io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "test assigned wake failure",
        )))
    }
}
