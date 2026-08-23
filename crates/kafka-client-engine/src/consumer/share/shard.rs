//! Synchronized share registry with unique host and cloneable port ownership.

use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use super::{
    port::ShareConsumerPort, registry::ShareConsumerRegistry, shard_wake::ShareConsumerShardWake,
};
use crate::clock::MonotonicClock;
use crate::completion::{CompletionRegistryError, NotifierJoin};

pub(super) struct ShareConsumerShardState {
    registry_owner_share: Mutex<ShareConsumerRegistry>,
    share_admission_gate: AtomicBool,
    deferred_share_port_contention: AtomicBool,
    share_turn_notifier: Arc<dyn ShareConsumerShardWake>,
    membership_deadline_clock: Arc<MonotonicClock>,
}

pub(crate) struct ShareConsumerShardOwner {
    shared: Arc<ShareConsumerShardState>,
}

impl ShareConsumerShardOwner {
    pub(crate) fn new<W>(
        registry: ShareConsumerRegistry,
        clock: Arc<MonotonicClock>,
        wake: Arc<W>,
    ) -> Self
    where
        W: ShareConsumerShardWake,
    {
        let shared = Arc::new(ShareConsumerShardState {
            registry_owner_share: Mutex::new(registry),
            share_admission_gate: AtomicBool::new(false),
            deferred_share_port_contention: AtomicBool::new(false),
            share_turn_notifier: wake,
            membership_deadline_clock: clock,
        });
        Self { shared }
    }

    pub(crate) fn admission_port(&self) -> ShareConsumerPort {
        ShareConsumerPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_registry_for_host_turn(
        &self,
    ) -> Result<ShareConsumerRegistryGuard<'_>, ShareConsumerShardLockError> {
        if self
            .shared
            .deferred_share_port_contention
            .swap(false, Ordering::AcqRel)
        {
            return Err(ShareConsumerShardLockError::Contended);
        }
        self.shared.try_registry_raw()
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close_admission();
    }

    pub(crate) fn close_notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.shared
            .registry_owner_share
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close_notifier_thread_id()
    }

    pub(crate) fn stop_close_notifier(&self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.terminal_registry().stop_close_notifier()
    }

    pub(crate) fn take_close_notifier(&self) -> Option<NotifierJoin> {
        self.terminal_registry().take_close_notifier()
    }

    pub(crate) fn recover_after_driver_shutdown(
        &self,
    ) -> Result<(), super::ShareMembershipHostError> {
        self.terminal_registry().recover_after_driver_shutdown()
    }

    pub(crate) fn terminal_registry(&self) -> ShareConsumerRegistryGuard<'_> {
        self.shared.close_admission();
        let registry = self
            .shared
            .registry_owner_share
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registry = ShareConsumerRegistryGuard(registry);
        registry.close_admission();
        registry
    }

    #[cfg(test)]
    pub(crate) fn lock_registry_for_test(&self) -> ShareConsumerRegistryGuard<'_> {
        let registry = self
            .shared
            .registry_owner_share
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShareConsumerRegistryGuard(registry)
    }
}

impl ShareConsumerShardState {
    pub(super) fn admission_is_closed(&self) -> bool {
        self.share_admission_gate.load(Ordering::Acquire)
    }

    pub(super) fn close_admission(&self) {
        self.share_admission_gate.store(true, Ordering::Release);
    }

    pub(super) fn try_registry(
        &self,
    ) -> Result<ShareConsumerRegistryGuard<'_>, ShareConsumerShardLockError> {
        let result = self.try_registry_raw();
        if matches!(&result, Err(ShareConsumerShardLockError::Contended)) {
            self.deferred_share_port_contention
                .store(true, Ordering::Release);
        }
        result
    }

    pub(super) fn control_registry(&self) -> ShareConsumerRegistryGuard<'_> {
        let registry = self
            .registry_owner_share
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShareConsumerRegistryGuard(registry)
    }

    fn try_registry_raw(
        &self,
    ) -> Result<ShareConsumerRegistryGuard<'_>, ShareConsumerShardLockError> {
        match self.registry_owner_share.try_lock() {
            Ok(registry) => Ok(ShareConsumerRegistryGuard(registry)),
            Err(TryLockError::WouldBlock) => Err(ShareConsumerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ShareConsumerShardLockError::Poisoned),
        }
    }

    pub(super) fn request_turn(&self) -> Result<(), super::ShareConsumerShardWakeError> {
        self.share_turn_notifier.request_share_turn()
    }

    pub(super) fn clock(&self) -> &Arc<MonotonicClock> {
        &self.membership_deadline_clock
    }
}

pub(crate) struct ShareConsumerRegistryGuard<'owner>(MutexGuard<'owner, ShareConsumerRegistry>);

impl Deref for ShareConsumerRegistryGuard<'_> {
    type Target = ShareConsumerRegistry;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ShareConsumerRegistryGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerShardLockError {
    Contended,
    Poisoned,
}
