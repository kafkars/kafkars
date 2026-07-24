//! Atomic admission fencing and synchronized assigned-consumer access.

mod notification;

#[cfg(test)]
mod notification_test;

use std::sync::{
    Arc, Mutex, MutexGuard, TryLockError,
    atomic::{AtomicBool, Ordering},
};

use crate::clock::MonotonicClock;

use super::super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_model::AssignedConsumerOwnerError,
    fetch_store::FetchDelivery,
};
use super::{
    close_observer::AssignedConsumerCloseObserver,
    completion::AssignedConsumerRecvPublisher,
    delivery::AssignedConsumerDelivery,
    reclaim::AssignedConsumerReclaimRejection,
    recv::{AssignedConsumerRecvSignal, AssignedConsumerRecvWait},
    shard::AssignedConsumerShardLockError,
    wake::AssignedConsumerShardWake,
};

pub(super) struct AssignedConsumerShardState {
    owner: Mutex<Option<AssignedConsumerOwner>>,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) wake: Arc<dyn AssignedConsumerShardWake>,
    pub(super) recv_signal: Arc<AssignedConsumerRecvSignal>,
    recv_publisher: AssignedConsumerRecvPublisher,
    admission_closed: AtomicBool,
}

impl AssignedConsumerShardState {
    pub(super) fn new(
        owner: AssignedConsumerOwner,
        clock: Arc<MonotonicClock>,
        wake: Arc<dyn AssignedConsumerShardWake>,
        recv_publisher: AssignedConsumerRecvPublisher,
    ) -> Self {
        Self {
            owner: Mutex::new(Some(owner)),
            clock,
            wake,
            recv_signal: Arc::new(AssignedConsumerRecvSignal::new()),
            recv_publisher,
            admission_closed: AtomicBool::new(false),
        }
    }

    pub(super) fn publish_assigned_admission_closed(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }

    pub(super) fn close_assigned_admission(&self) -> Result<(), AssignedConsumerShardLockError> {
        self.publish_assigned_admission_closed();
        match self.owner() {
            Ok(guard) => self.finish_owner_lock(guard, Ok(()), AssignedConsumerRecvWait::Change),
            Err(error) => {
                self.request_recv_notification(AssignedConsumerRecvWait::Change);
                Err(error)
            }
        }
    }

    pub(super) fn assigned_admission_is_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(super) fn try_with_owner<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        let mut guard = self.try_owner()?;
        let result = match guard.as_mut() {
            Some(owner) => Ok(operation(owner)),
            None => Err(AssignedConsumerShardLockError::OwnerMissing),
        };
        self.finish_owner_lock(guard, result, AssignedConsumerRecvWait::Unlock)
    }

    pub(super) fn inspect_owner<T>(
        &self,
        inspect: impl FnOnce(&AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        let guard = self.owner()?;
        let result = match guard.as_ref() {
            Some(owner) => Ok(inspect(owner)),
            None => Err(AssignedConsumerShardLockError::OwnerMissing),
        };
        self.finish_owner_lock(guard, result, AssignedConsumerRecvWait::Unlock)
    }

    pub(super) fn try_admit_with_owner<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> T,
    ) -> Result<Option<T>, AssignedConsumerShardLockError> {
        let mut guard = self.try_owner()?;
        let result = if self.assigned_admission_is_closed() {
            Ok(None)
        } else {
            match guard.as_mut() {
                Some(owner) => Ok(Some(operation(owner))),
                None => Err(AssignedConsumerShardLockError::OwnerMissing),
            }
        };
        self.finish_owner_lock(guard, result, AssignedConsumerRecvWait::Unlock)
    }

    pub(super) fn begin_assigned_close(
        &self,
    ) -> Result<
        Option<Result<AssignedConsumerCloseObserver, AssignedConsumerOwnerError>>,
        AssignedConsumerShardLockError,
    > {
        let mut guard = self.try_owner()?;
        let result = if self.assigned_admission_is_closed() {
            Ok(None)
        } else {
            match guard.as_mut() {
                Some(owner) => Ok(Some(owner.begin_close())),
                None => Err(AssignedConsumerShardLockError::OwnerMissing),
            }
        };
        let accepted = result
            .as_ref()
            .is_ok_and(|close| close.as_ref().is_some_and(Result::is_ok));
        if accepted {
            self.publish_assigned_admission_closed();
        }
        let wake = if accepted {
            AssignedConsumerRecvWait::Change
        } else {
            AssignedConsumerRecvWait::Unlock
        };
        self.finish_owner_lock(guard, result, wake)
    }

    #[expect(
        clippy::result_large_err,
        reason = "pre-transfer rejection must return the exact linear delivery without allocation"
    )]
    pub(super) fn reclaim_assigned_delivery(
        &self,
        delivery: FetchDelivery,
    ) -> Result<Result<(), AssignedConsumerOwnerError>, AssignedConsumerReclaimRejection> {
        let mut guard = match self.try_owner() {
            Ok(guard) => guard,
            Err(reason) => return Err(AssignedConsumerReclaimRejection::new(reason, delivery)),
        };
        let result = match guard.as_mut() {
            Some(owner) => Ok(owner.reclaim_delivery(delivery)),
            None => Err(AssignedConsumerReclaimRejection::new(
                AssignedConsumerShardLockError::OwnerMissing,
                delivery,
            )),
        };
        self.finish_owner_lock(guard, result, AssignedConsumerRecvWait::Unlock)
    }

    /// Returns a public batch lease without losing it to transient contention.
    pub(super) fn return_assigned_delivery(&self, delivery: AssignedConsumerDelivery) {
        let mut guard = match self.owner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let returned_to_owner = if let Some(owner) = guard.as_mut() {
            let _reclaimed = owner.reclaim_delivery(delivery.into_lease());
            true
        } else {
            false
        };
        drop(guard);
        if returned_to_owner {
            let _wake = self.wake.request_assigned_turn();
        }
        self.request_recv_notification(AssignedConsumerRecvWait::Unlock);
    }

    /// Consumes the failed owner after the caller has destroyed the unique driver.
    pub(super) fn take_owner_for_post_driver_recovery(
        &self,
    ) -> Result<super::super::AssignedConsumerRecoveryReport, AssignedConsumerShardLockError> {
        let (mut guard, lock_was_poisoned) = match self.owner.lock() {
            Ok(guard) => (guard, false),
            Err(poisoned) => (poisoned.into_inner(), true),
        };
        let result = match guard.take() {
            Some(owner) => {
                let audit = owner.recovery_audit();
                let release = owner.release_assigned_after_driver_shutdown();
                Ok(super::super::AssignedConsumerRecoveryReport::new(
                    audit,
                    release,
                    lock_was_poisoned,
                ))
            }
            None => Err(AssignedConsumerShardLockError::OwnerMissing),
        };
        let wake = if result.is_ok() {
            AssignedConsumerRecvWait::Change
        } else {
            AssignedConsumerRecvWait::Unlock
        };
        self.finish_owner_lock(guard, result, wake)
    }

    #[cfg(test)]
    pub(super) fn lock_for_test(&self) -> MutexGuard<'_, Option<AssignedConsumerOwner>> {
        match self.owner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(super) fn owner(
        &self,
    ) -> Result<MutexGuard<'_, Option<AssignedConsumerOwner>>, AssignedConsumerShardLockError> {
        self.owner
            .lock()
            .map_err(|_poisoned| AssignedConsumerShardLockError::Poisoned)
    }

    pub(super) fn try_owner(
        &self,
    ) -> Result<MutexGuard<'_, Option<AssignedConsumerOwner>>, AssignedConsumerShardLockError> {
        match self.owner.try_lock() {
            Ok(guard) => Ok(guard),
            Err(TryLockError::WouldBlock) => Err(AssignedConsumerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AssignedConsumerShardLockError::Poisoned),
        }
    }
}
