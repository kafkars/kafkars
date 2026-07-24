//! Atomic admission fencing and synchronized assigned-consumer access.

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
    close_observer::AssignedConsumerCloseObserver, reclaim::AssignedConsumerReclaimRejection,
    shard::AssignedConsumerShardLockError, wake::AssignedConsumerShardWake,
};

pub(super) struct AssignedConsumerShardState {
    owner: Mutex<Option<AssignedConsumerOwner>>,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) wake: Arc<dyn AssignedConsumerShardWake>,
    admission_closed: AtomicBool,
}

impl AssignedConsumerShardState {
    pub(super) fn new(
        owner: AssignedConsumerOwner,
        clock: Arc<MonotonicClock>,
        wake: Arc<dyn AssignedConsumerShardWake>,
    ) -> Self {
        Self {
            owner: Mutex::new(Some(owner)),
            clock,
            wake,
            admission_closed: AtomicBool::new(false),
        }
    }

    pub(super) fn publish_assigned_admission_closed(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }

    pub(super) fn close_assigned_admission(&self) -> Result<(), AssignedConsumerShardLockError> {
        self.publish_assigned_admission_closed();
        let _guard = self.owner()?;
        Ok(())
    }

    pub(super) fn assigned_admission_is_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(super) fn try_with_owner<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        let mut guard = self.try_owner()?;
        let Some(owner) = guard.as_mut() else {
            return Err(AssignedConsumerShardLockError::OwnerMissing);
        };
        Ok(operation(owner))
    }

    pub(super) fn inspect_owner<T>(
        &self,
        inspect: impl FnOnce(&AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        let guard = self.owner()?;
        let Some(owner) = guard.as_ref() else {
            return Err(AssignedConsumerShardLockError::OwnerMissing);
        };
        Ok(inspect(owner))
    }

    pub(super) fn try_admit_with_owner<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> T,
    ) -> Result<Option<T>, AssignedConsumerShardLockError> {
        let mut guard = self.try_owner()?;
        if self.assigned_admission_is_closed() {
            return Ok(None);
        }
        let Some(owner) = guard.as_mut() else {
            return Err(AssignedConsumerShardLockError::OwnerMissing);
        };
        Ok(Some(operation(owner)))
    }

    pub(super) fn begin_assigned_close(
        &self,
    ) -> Result<
        Option<Result<AssignedConsumerCloseObserver, AssignedConsumerOwnerError>>,
        AssignedConsumerShardLockError,
    > {
        let mut guard = self.try_owner()?;
        if self.assigned_admission_is_closed() {
            return Ok(None);
        }
        let Some(owner) = guard.as_mut() else {
            return Err(AssignedConsumerShardLockError::OwnerMissing);
        };
        let result = owner.begin_close();
        if result.is_ok() {
            self.publish_assigned_admission_closed();
        }
        Ok(Some(result))
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
        let Some(owner) = guard.as_mut() else {
            return Err(AssignedConsumerReclaimRejection::new(
                AssignedConsumerShardLockError::OwnerMissing,
                delivery,
            ));
        };
        Ok(owner.reclaim_delivery(delivery))
    }

    /// Consumes the failed owner after the caller has destroyed the unique driver.
    pub(super) fn take_owner_for_post_driver_recovery(
        &self,
    ) -> Result<super::super::AssignedConsumerRecoveryReport, AssignedConsumerShardLockError> {
        let (mut guard, lock_was_poisoned) = match self.owner.lock() {
            Ok(guard) => (guard, false),
            Err(poisoned) => (poisoned.into_inner(), true),
        };
        let Some(owner) = guard.take() else {
            return Err(AssignedConsumerShardLockError::OwnerMissing);
        };
        let audit = owner.recovery_audit();
        let release = owner.release_assigned_after_driver_shutdown();
        Ok(super::super::AssignedConsumerRecoveryReport::new(
            audit,
            release,
            lock_was_poisoned,
        ))
    }

    #[cfg(test)]
    pub(super) fn lock_for_test(&self) -> MutexGuard<'_, Option<AssignedConsumerOwner>> {
        match self.owner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn owner(
        &self,
    ) -> Result<MutexGuard<'_, Option<AssignedConsumerOwner>>, AssignedConsumerShardLockError> {
        self.owner
            .lock()
            .map_err(|_poisoned| AssignedConsumerShardLockError::Poisoned)
    }

    fn try_owner(
        &self,
    ) -> Result<MutexGuard<'_, Option<AssignedConsumerOwner>>, AssignedConsumerShardLockError> {
        match self.owner.try_lock() {
            Ok(guard) => Ok(guard),
            Err(TryLockError::WouldBlock) => Err(AssignedConsumerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AssignedConsumerShardLockError::Poisoned),
        }
    }
}
