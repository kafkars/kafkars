//! Linear synchronized ownership of one incremental configuration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{IncrementalAlterConfigsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    IncrementalAlterConfigsAdmissionErrorKind, IncrementalAlterConfigsHost,
    IncrementalAlterConfigsHostError, host::IncrementalAlterConfigsAdmission,
    model::IncrementalAlterConfigsRetention,
};

pub(crate) trait IncrementalAlterConfigsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), IncrementalAlterConfigsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct IncrementalAlterConfigsShardWakeError {
    source: io::Error,
}

impl IncrementalAlterConfigsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for IncrementalAlterConfigsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "IncrementalAlterConfigs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for IncrementalAlterConfigsShardWakeError {}

struct IncrementalAlterConfigsShardState {
    host: Mutex<IncrementalAlterConfigsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn IncrementalAlterConfigsShardWake>,
}

#[derive(Clone)]
pub(crate) struct IncrementalAlterConfigsAdmissionPort {
    shared: Arc<IncrementalAlterConfigsShardState>,
}

impl IncrementalAlterConfigsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: IncrementalAlterConfigsPlan,
        retention: IncrementalAlterConfigsRetention,
    ) -> Result<IncrementalAlterConfigsAdmission, IncrementalAlterConfigsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(IncrementalAlterConfigsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(IncrementalAlterConfigsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(IncrementalAlterConfigsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retention)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(IncrementalAlterConfigsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), IncrementalAlterConfigsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct IncrementalAlterConfigsShardOwner {
    shared: Arc<IncrementalAlterConfigsShardState>,
}

impl IncrementalAlterConfigsShardOwner {
    pub(crate) fn new<W>(host: IncrementalAlterConfigsHost, wake: Arc<W>) -> Self
    where
        W: IncrementalAlterConfigsShardWake,
    {
        Self {
            shared: Arc::new(IncrementalAlterConfigsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> IncrementalAlterConfigsAdmissionPort {
        IncrementalAlterConfigsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, IncrementalAlterConfigsHost>, IncrementalAlterConfigsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(IncrementalAlterConfigsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(IncrementalAlterConfigsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut IncrementalAlterConfigsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, IncrementalAlterConfigsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl IncrementalAlterConfigsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, IncrementalAlterConfigsHost>, IncrementalAlterConfigsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| IncrementalAlterConfigsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IncrementalAlterConfigsShardLockError {
    Contended,
    Poisoned,
}
