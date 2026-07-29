//! Linear synchronized ownership of one broker-unregistration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{Moment, UnregisterBrokerPlan};

use crate::clock::OperationDeadline;

use super::{
    UnregisterBrokerAdmissionErrorKind, UnregisterBrokerHost, UnregisterBrokerHostError,
    host::UnregisterBrokerAdmission,
};

pub(crate) trait UnregisterBrokerShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), UnregisterBrokerShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct UnregisterBrokerShardWakeError {
    source: io::Error,
}

impl UnregisterBrokerShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for UnregisterBrokerShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UnregisterBroker shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for UnregisterBrokerShardWakeError {}

struct UnregisterBrokerShardState {
    host: Mutex<UnregisterBrokerHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn UnregisterBrokerShardWake>,
}

#[derive(Clone)]
pub(crate) struct UnregisterBrokerAdmissionPort {
    shared: Arc<UnregisterBrokerShardState>,
}

impl UnregisterBrokerAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: UnregisterBrokerPlan,
    ) -> Result<UnregisterBrokerAdmission, UnregisterBrokerAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(UnregisterBrokerAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(UnregisterBrokerAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(UnregisterBrokerAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(UnregisterBrokerHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), UnregisterBrokerShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct UnregisterBrokerShardOwner {
    shared: Arc<UnregisterBrokerShardState>,
}

impl UnregisterBrokerShardOwner {
    pub(crate) fn new<W>(host: UnregisterBrokerHost, wake: Arc<W>) -> Self
    where
        W: UnregisterBrokerShardWake,
    {
        Self {
            shared: Arc::new(UnregisterBrokerShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> UnregisterBrokerAdmissionPort {
        UnregisterBrokerAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, UnregisterBrokerHost>, UnregisterBrokerShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(UnregisterBrokerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(UnregisterBrokerShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut UnregisterBrokerHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, UnregisterBrokerHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl UnregisterBrokerShardState {
    fn host(&self) -> Result<MutexGuard<'_, UnregisterBrokerHost>, UnregisterBrokerShardLockError> {
        self.host
            .lock()
            .map_err(|_| UnregisterBrokerShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerShardLockError {
    Contended,
    Poisoned,
}
