//! Synchronized single owner and domain-neutral wake adaptation.

use std::sync::{
    Arc, Mutex, MutexGuard, TryLockError,
    atomic::{AtomicBool, Ordering},
};

use crate::{clock::MonotonicClock, driver::ReactorWake};

use super::{TransactionInitializationAdmissionPort, TransactionInitializationHost};

pub(super) struct TransactionInitializationShardState {
    host: Mutex<TransactionInitializationHost>,
    admission_closed: AtomicBool,
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
}

pub(crate) struct TransactionInitializationShardOwner {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionInitializationShardOwner {
    pub(crate) fn new(
        host: TransactionInitializationHost,
        clock: Arc<MonotonicClock>,
        wake: Arc<ReactorWake>,
    ) -> Self {
        Self {
            shared: Arc::new(TransactionInitializationShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                clock,
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> TransactionInitializationAdmissionPort {
        TransactionInitializationAdmissionPort::new(Arc::clone(&self.shared))
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, TransactionInitializationHost>,
        TransactionInitializationShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(TransactionInitializationShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(TransactionInitializationShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut TransactionInitializationHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, TransactionInitializationHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }

    pub(crate) fn notifier_thread_id(&self) -> Option<std::thread::ThreadId> {
        self.shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .notifier_thread_id()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionInitializationShardLockError {
    Contended,
    Poisoned,
}

impl TransactionInitializationShardState {
    pub(super) fn is_closed(&self) -> bool {
        self.admission_closed.load(Ordering::Acquire)
    }

    pub(super) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, TransactionInitializationHost>,
        TryLockError<MutexGuard<'_, TransactionInitializationHost>>,
    > {
        self.host.try_lock()
    }

    pub(super) fn clock(&self) -> &MonotonicClock {
        &self.clock
    }

    pub(super) fn wake(&self) -> &ReactorWake {
        &self.wake
    }

    pub(super) fn close(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }
}
