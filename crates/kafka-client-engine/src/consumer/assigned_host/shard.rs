//! Nonblocking synchronization and call-boundary deadlines for one assigned consumer.

use std::sync::Arc;

#[cfg(test)]
use std::sync::MutexGuard;

use crate::clock::MonotonicClock;

use super::super::{
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_model::{
        AssignedConsumerOwnerBuildError, AssignedConsumerOwnerError, AssignedConsumerOwnerLimits,
        AssignedConsumerOwnerSettings,
    },
};
use super::{state::AssignedConsumerShardState, wake::AssignedConsumerShardWake};

/// Unique host-side capability for one synchronized assigned consumer.
pub(crate) struct AssignedConsumerShardOwner {
    shared: Arc<AssignedConsumerShardState>,
}

/// Non-clone application-side capability for the same concrete lifecycle.
pub(crate) struct AssignedConsumerPort {
    pub(super) shared: Arc<AssignedConsumerShardState>,
}

impl AssignedConsumerShardOwner {
    pub(super) fn new<W>(
        clock: Arc<MonotonicClock>,
        settings: AssignedConsumerOwnerSettings,
        limits: AssignedConsumerOwnerLimits,
        wake: Arc<W>,
    ) -> Result<(Self, AssignedConsumerPort), AssignedConsumerOwnerBuildError>
    where
        W: AssignedConsumerShardWake,
    {
        let owner = AssignedConsumerOwner::new(Arc::clone(&clock), settings, limits)?;
        let shared = Arc::new(AssignedConsumerShardState::new(owner, clock, wake));
        Ok((
            Self {
                shared: Arc::clone(&shared),
            },
            AssignedConsumerPort { shared },
        ))
    }

    pub(crate) fn try_with_owner<T>(
        &self,
        operation: impl FnOnce(&mut AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        self.shared.try_with_owner(operation)
    }

    pub(crate) fn begin_shutdown(
        &self,
        owner: &mut AssignedConsumerOwner,
    ) -> Result<AssignedConsumerShutdownStart, AssignedConsumerOwnerError> {
        self.shared.publish_assigned_admission_closed();
        if owner.close_started() {
            return Ok(AssignedConsumerShutdownStart::AlreadyStarted);
        }
        match owner.begin_close() {
            Ok(()) => Ok(AssignedConsumerShutdownStart::Started),
            Err(AssignedConsumerOwnerError::EffectsPending) => {
                Ok(AssignedConsumerShutdownStart::Pending)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn inspect_terminal<T>(
        &self,
        inspect: impl FnOnce(&AssignedConsumerOwner) -> T,
    ) -> Result<T, AssignedConsumerShardLockError> {
        self.shared.inspect_owner(inspect)
    }

    /// Takes the failed owner only after the unique driver has been destroyed.
    pub(crate) fn take_assigned_owner_after_driver_shutdown(
        &self,
    ) -> Result<super::super::AssignedConsumerRecoveryReport, AssignedConsumerShardLockError> {
        self.shared.take_owner_for_post_driver_recovery()
    }

    pub(crate) fn close_assigned_admission(&self) -> Result<(), AssignedConsumerShardLockError> {
        self.shared.close_assigned_admission()
    }

    #[cfg(test)]
    pub(crate) fn lock_for_test(&self) -> MutexGuard<'_, Option<AssignedConsumerOwner>> {
        self.shared.lock_for_test()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerShardLockError {
    Contended,
    Poisoned,
    OwnerMissing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerShutdownStart {
    Started,
    Pending,
    AlreadyStarted,
}
