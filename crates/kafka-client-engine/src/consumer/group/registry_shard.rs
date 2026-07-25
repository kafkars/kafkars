//! Synchronized private admission and unique host ownership of the group registry.

use std::sync::{
    Arc, Mutex, MutexGuard, TryLockError,
    atomic::{AtomicBool, Ordering},
};

use crate::clock::MonotonicClock;

use super::{
    registry::GroupConsumerRegistry, registry_port::GroupConsumerPort,
    registry_wake::GroupConsumerShardWake,
};

pub(super) struct GroupConsumerShardState {
    registry_owner: Mutex<GroupConsumerRegistry>,
    admission_fence: AtomicBool,
    reactor_wake: Arc<dyn GroupConsumerShardWake>,
}

/// Unique embedded-host capability over the synchronized registry.
pub(crate) struct GroupConsumerShardOwner {
    shared: Arc<GroupConsumerShardState>,
}

impl GroupConsumerShardOwner {
    pub(crate) fn new<W>(
        registry: GroupConsumerRegistry,
        clock: Arc<MonotonicClock>,
        wake: Arc<W>,
    ) -> (Self, GroupConsumerPort)
    where
        W: GroupConsumerShardWake,
    {
        let shared = Arc::new(GroupConsumerShardState {
            registry_owner: Mutex::new(registry),
            admission_fence: AtomicBool::new(false),
            reactor_wake: wake,
        });
        (
            Self {
                shared: Arc::clone(&shared),
            },
            GroupConsumerPort { shared, clock },
        )
    }

    pub(crate) fn try_registry(
        &self,
    ) -> Result<MutexGuard<'_, GroupConsumerRegistry>, GroupConsumerShardLockError> {
        self.shared.try_registry()
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close_admission();
    }

    pub(crate) fn terminal_registry(&self) -> MutexGuard<'_, GroupConsumerRegistry> {
        self.shared.close_admission();
        let mut registry = self
            .shared
            .registry_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.close_admission();
        registry
    }

    #[cfg(test)]
    pub(crate) fn lock_registry_for_test(&self) -> MutexGuard<'_, GroupConsumerRegistry> {
        self.shared
            .registry_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl GroupConsumerShardState {
    pub(super) fn admission_is_closed(&self) -> bool {
        self.admission_fence.load(Ordering::Acquire)
    }

    pub(super) fn close_admission(&self) {
        self.admission_fence.store(true, Ordering::Release);
    }

    pub(super) fn try_registry(
        &self,
    ) -> Result<MutexGuard<'_, GroupConsumerRegistry>, GroupConsumerShardLockError> {
        match self.registry_owner.try_lock() {
            Ok(registry) => Ok(registry),
            Err(TryLockError::WouldBlock) => Err(GroupConsumerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(GroupConsumerShardLockError::Poisoned),
        }
    }

    pub(super) fn request_turn(
        &self,
    ) -> Result<(), super::registry_wake::GroupConsumerShardWakeError> {
        self.reactor_wake.request_group_turn()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerShardLockError {
    Contended,
    Poisoned,
}
