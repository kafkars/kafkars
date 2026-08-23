//! Synchronized share registry with unique host and cloneable port ownership.

use std::sync::{
    Arc, Mutex, MutexGuard, TryLockError,
    atomic::{AtomicBool, Ordering},
};

use super::{
    port::ShareConsumerPort, registry::ShareConsumerRegistry, shard_wake::ShareConsumerShardWake,
};
use crate::clock::MonotonicClock;
use crate::completion::{CompletionRegistryError, NotifierJoin};
use crate::consumer::share_recv::{
    ShareConsumerRecvNotifier, ShareConsumerRecvSignal, ShareConsumerRecvWait,
};

pub(super) struct ShareConsumerShardState {
    registry_owner_share: Mutex<ShareConsumerRegistry>,
    share_admission_gate: AtomicBool,
    deferred_share_port_contention: AtomicBool,
    share_turn_notifier: Arc<dyn ShareConsumerShardWake>,
    membership_deadline_clock: Arc<MonotonicClock>,
    share_recv_signal: Arc<ShareConsumerRecvSignal>,
    share_recv_publisher: crate::consumer::share_recv::ShareConsumerRecvPublisher,
}

pub(crate) struct ShareConsumerShardOwner {
    shared: Arc<ShareConsumerShardState>,
    recv_notifier: Option<ShareConsumerRecvNotifier>,
}

impl ShareConsumerShardOwner {
    pub(crate) fn new<W>(
        mut registry: ShareConsumerRegistry,
        clock: Arc<MonotonicClock>,
        wake: Arc<W>,
    ) -> Self
    where
        W: ShareConsumerShardWake,
    {
        let notifications = registry
            .recv_notifications
            .take()
            .unwrap_or_else(|| unreachable!("share receive notifier transfers once"));
        let shared = Arc::new(ShareConsumerShardState {
            registry_owner_share: Mutex::new(registry),
            share_admission_gate: AtomicBool::new(false),
            deferred_share_port_contention: AtomicBool::new(false),
            share_turn_notifier: wake,
            membership_deadline_clock: clock,
            share_recv_signal: Arc::new(ShareConsumerRecvSignal::new()),
            share_recv_publisher: notifications.publisher,
        });
        Self {
            shared,
            recv_notifier: Some(notifications.notifier),
        }
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

    pub(crate) fn stop_acknowledgement_notifier(
        &self,
    ) -> Result<NotifierJoin, CompletionRegistryError> {
        self.terminal_registry().stop_acknowledgement_notifier()
    }

    pub(crate) fn take_acknowledgement_notifier(&self) -> Option<NotifierJoin> {
        self.terminal_registry().take_acknowledgement_notifier()
    }

    pub(crate) fn recover_after_driver_shutdown(
        &self,
    ) -> Result<(), super::ShareMembershipHostError> {
        self.terminal_registry().recover_after_driver_shutdown()
    }

    pub(crate) fn notify_recv_change(&self) {
        self.shared
            .request_share_recv_notification(ShareConsumerRecvWait::Change);
    }

    pub(crate) fn stop_recv_notifier(&mut self) -> Option<NotifierJoin> {
        let mut notifier = self.recv_notifier.take()?;
        notifier.stop()
    }

    #[cfg(test)]
    pub(crate) fn recv_registration_count(&self) -> usize {
        self.shared.share_recv_signal().registration_count()
    }

    pub(crate) fn terminal_registry(&self) -> ShareConsumerRegistryGuard<'_> {
        self.shared.close_admission();
        let registry = self
            .shared
            .registry_owner_share
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registry = ShareConsumerRegistryGuard::new(registry, &self.shared);
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
        ShareConsumerRegistryGuard::new(registry, &self.shared)
    }
}

impl ShareConsumerShardState {
    pub(super) fn admission_is_closed(&self) -> bool {
        self.share_admission_gate.load(Ordering::Acquire)
    }

    pub(super) fn close_admission(&self) {
        self.share_admission_gate.store(true, Ordering::Release);
        self.request_share_recv_notification(ShareConsumerRecvWait::Change);
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
        ShareConsumerRegistryGuard::new(registry, self)
    }

    fn try_registry_raw(
        &self,
    ) -> Result<ShareConsumerRegistryGuard<'_>, ShareConsumerShardLockError> {
        match self.registry_owner_share.try_lock() {
            Ok(registry) => Ok(ShareConsumerRegistryGuard::new(registry, self)),
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

    pub(super) fn share_recv_signal(&self) -> &Arc<ShareConsumerRecvSignal> {
        &self.share_recv_signal
    }

    pub(super) fn share_recv_publisher(
        &self,
    ) -> &crate::consumer::share_recv::ShareConsumerRecvPublisher {
        &self.share_recv_publisher
    }
}

pub(crate) struct ShareConsumerRegistryGuard<'owner> {
    pub(super) registry: Option<MutexGuard<'owner, ShareConsumerRegistry>>,
    pub(super) shared: &'owner ShareConsumerShardState,
}

impl<'owner> ShareConsumerRegistryGuard<'owner> {
    pub(super) const fn new(
        registry: MutexGuard<'owner, ShareConsumerRegistry>,
        shared: &'owner ShareConsumerShardState,
    ) -> Self {
        Self {
            registry: Some(registry),
            shared,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerShardLockError {
    Contended,
    Poisoned,
}
