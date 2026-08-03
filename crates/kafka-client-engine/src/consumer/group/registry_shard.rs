//! Synchronized private admission and unique host ownership of the group registry.

use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::clock::MonotonicClock;
use crate::consumer::group_recv::{
    GroupConsumerRecvNotifier, GroupConsumerRecvSignal, GroupConsumerRecvWait,
};

use super::{
    classic_group_fetch::ClassicGroupFetchDelivery, registry::GroupConsumerRegistry,
    registry_port::GroupConsumerPort, registry_wake::GroupConsumerShardWake,
};

pub(super) struct GroupConsumerShardState {
    registry_owner: Mutex<GroupConsumerRegistry>,
    admission_fence: AtomicBool,
    port_contention_handoff: AtomicBool,
    reactor_wake: Arc<dyn GroupConsumerShardWake>,
    group_recv_signal: Arc<GroupConsumerRecvSignal>,
    group_recv_publisher: crate::consumer::group_recv::GroupConsumerRecvPublisher,
}

/// Unique embedded-host capability over the synchronized registry.
pub(crate) struct GroupConsumerShardOwner {
    shared: Arc<GroupConsumerShardState>,
    recv_notifier: Option<GroupConsumerRecvNotifier>,
}

impl GroupConsumerShardOwner {
    pub(crate) fn new<W>(
        mut registry: GroupConsumerRegistry,
        clock: Arc<MonotonicClock>,
        wake: Arc<W>,
    ) -> (Self, GroupConsumerPort)
    where
        W: GroupConsumerShardWake,
    {
        let notifications = registry
            .recv_notifications
            .take()
            .unwrap_or_else(|| unreachable!("group receive notifier transfers once"));
        let shared = Arc::new(GroupConsumerShardState {
            registry_owner: Mutex::new(registry),
            admission_fence: AtomicBool::new(false),
            port_contention_handoff: AtomicBool::new(false),
            reactor_wake: wake,
            group_recv_signal: Arc::new(GroupConsumerRecvSignal::new()),
            group_recv_publisher: notifications.publisher,
        });
        (
            Self {
                shared: Arc::clone(&shared),
                recv_notifier: Some(notifications.notifier),
            },
            GroupConsumerPort { shared, clock },
        )
    }

    pub(crate) fn try_registry(
        &self,
    ) -> Result<GroupConsumerRegistryGuard<'_>, GroupConsumerShardLockError> {
        self.shared.try_registry_raw()
    }

    /// Gives one contended public port a complete host stage without registry ownership.
    pub(crate) fn try_registry_for_host_turn(
        &self,
    ) -> Result<GroupConsumerRegistryGuard<'_>, GroupConsumerShardLockError> {
        if self
            .shared
            .port_contention_handoff
            .swap(false, Ordering::AcqRel)
        {
            return Err(GroupConsumerShardLockError::Contended);
        }
        self.shared.try_registry_raw()
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close_admission();
    }

    pub(crate) fn terminal_registry(&self) -> GroupConsumerRegistryGuard<'_> {
        self.shared.close_admission();
        let registry = self
            .shared
            .registry_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registry = GroupConsumerRegistryGuard::new(registry, &self.shared);
        registry.close_admission();
        registry
    }

    #[cfg(test)]
    pub(crate) fn lock_registry_for_test(&self) -> GroupConsumerRegistryGuard<'_> {
        let registry = self
            .shared
            .registry_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        GroupConsumerRegistryGuard::new(registry, &self.shared)
    }

    pub(crate) fn notify_recv_change(&self) {
        self.shared
            .request_group_recv_notification(GroupConsumerRecvWait::Change);
    }

    pub(crate) fn stop_recv_notifier(&mut self) -> Option<crate::completion::NotifierJoin> {
        let mut notifier = self.recv_notifier.take()?;
        notifier.stop()
    }
}

impl GroupConsumerShardState {
    pub(super) fn admission_is_closed(&self) -> bool {
        self.admission_fence.load(Ordering::Acquire)
    }

    pub(super) fn close_admission(&self) {
        self.admission_fence.store(true, Ordering::Release);
        self.request_group_recv_notification(GroupConsumerRecvWait::Change);
    }

    pub(super) fn try_registry(
        &self,
    ) -> Result<GroupConsumerRegistryGuard<'_>, GroupConsumerShardLockError> {
        let result = self.try_registry_raw();
        if matches!(&result, Err(GroupConsumerShardLockError::Contended)) {
            self.port_contention_handoff.store(true, Ordering::Release);
        }
        result
    }

    fn try_registry_raw(
        &self,
    ) -> Result<GroupConsumerRegistryGuard<'_>, GroupConsumerShardLockError> {
        match self.registry_owner.try_lock() {
            Ok(registry) => Ok(GroupConsumerRegistryGuard::new(registry, self)),
            Err(TryLockError::WouldBlock) => Err(GroupConsumerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(GroupConsumerShardLockError::Poisoned),
        }
    }

    pub(super) fn registry(
        &self,
    ) -> Result<GroupConsumerRegistryGuard<'_>, GroupConsumerShardLockError> {
        self.registry_owner
            .lock()
            .map(|registry| GroupConsumerRegistryGuard::new(registry, self))
            .map_err(|_error| GroupConsumerShardLockError::Poisoned)
    }

    pub(super) fn request_turn(
        &self,
    ) -> Result<(), super::registry_wake::GroupConsumerShardWakeError> {
        self.reactor_wake.request_group_turn()
    }

    pub(super) fn group_recv_signal(&self) -> &Arc<GroupConsumerRecvSignal> {
        &self.group_recv_signal
    }

    pub(super) fn group_recv_publisher(
        &self,
    ) -> &crate::consumer::group_recv::GroupConsumerRecvPublisher {
        &self.group_recv_publisher
    }

    /// Returns an external byte lease without losing it to transient contention.
    pub(super) fn return_delivery_blocking(&self, delivery: ClassicGroupFetchDelivery) {
        let registry = self
            .registry_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registry = GroupConsumerRegistryGuard::new(registry, self);
        let returned_to_owner = registry.reclaim_delivery(delivery).is_ok();
        drop(registry);
        if returned_to_owner {
            let _wake_result = self.request_turn();
        }
    }
}

pub(crate) struct GroupConsumerRegistryGuard<'owner> {
    registry: Option<MutexGuard<'owner, GroupConsumerRegistry>>,
    shared: &'owner GroupConsumerShardState,
}

impl<'owner> GroupConsumerRegistryGuard<'owner> {
    fn new(
        registry: MutexGuard<'owner, GroupConsumerRegistry>,
        shared: &'owner GroupConsumerShardState,
    ) -> Self {
        Self {
            registry: Some(registry),
            shared,
        }
    }
}

impl Deref for GroupConsumerRegistryGuard<'_> {
    type Target = GroupConsumerRegistry;

    fn deref(&self) -> &Self::Target {
        self.registry
            .as_deref()
            .unwrap_or_else(|| unreachable!("registry guard is present before Drop"))
    }
}

impl DerefMut for GroupConsumerRegistryGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.registry
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("registry guard is present before Drop"))
    }
}

impl Drop for GroupConsumerRegistryGuard<'_> {
    fn drop(&mut self) {
        drop(self.registry.take());
        self.shared
            .request_group_recv_notification(GroupConsumerRecvWait::Unlock);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerShardLockError {
    Contended,
    Poisoned,
}

impl GroupConsumerShardLockError {
    pub(in crate::consumer) const fn is_contended(self) -> bool {
        matches!(self, Self::Contended)
    }

    pub(in crate::consumer) const fn is_poisoned(self) -> bool {
        matches!(self, Self::Poisoned)
    }
}
