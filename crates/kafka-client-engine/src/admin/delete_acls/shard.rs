//! Linear synchronized ownership of one Admin `DeleteAcls` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteAclsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteAclsAdmissionErrorKind, DeleteAclsHost, DeleteAclsHostError, host::DeleteAclsAdmission,
};

pub(crate) trait DeleteAclsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteAclsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteAclsShardWakeError {
    source: io::Error,
}

impl DeleteAclsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteAclsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteAcls shard wake failed: {}", self.source)
    }
}

impl std::error::Error for DeleteAclsShardWakeError {}

struct DeleteAclsShardState {
    host: Mutex<DeleteAclsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteAclsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteAclsAdmissionPort {
    shared: Arc<DeleteAclsShardState>,
}

impl DeleteAclsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteAclsPlan,
    ) -> Result<DeleteAclsAdmission, DeleteAclsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteAclsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteAclsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteAclsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(DeleteAclsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteAclsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteAclsShardOwner {
    shared: Arc<DeleteAclsShardState>,
}

impl DeleteAclsShardOwner {
    pub(crate) fn new<W>(host: DeleteAclsHost, wake: Arc<W>) -> Self
    where
        W: DeleteAclsShardWake,
    {
        Self {
            shared: Arc::new(DeleteAclsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteAclsAdmissionPort {
        DeleteAclsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteAclsHost>, DeleteAclsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DeleteAclsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DeleteAclsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteAclsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteAclsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteAclsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DeleteAclsHost>, DeleteAclsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DeleteAclsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsShardLockError {
    Contended,
    Poisoned,
}
