//! Linear synchronized ownership of one Admin `CreateAcls` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{CreateAclsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    CreateAclsAdmissionErrorKind, CreateAclsHost, CreateAclsHostError, host::CreateAclsAdmission,
};

pub(crate) trait CreateAclsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), CreateAclsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct CreateAclsShardWakeError {
    source: io::Error,
}

impl CreateAclsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for CreateAclsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CreateAcls shard wake failed: {}", self.source)
    }
}

impl std::error::Error for CreateAclsShardWakeError {}

struct CreateAclsShardState {
    host: Mutex<CreateAclsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn CreateAclsShardWake>,
}

#[derive(Clone)]
pub(crate) struct CreateAclsAdmissionPort {
    shared: Arc<CreateAclsShardState>,
}

impl CreateAclsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreateAclsPlan,
    ) -> Result<CreateAclsAdmission, CreateAclsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(CreateAclsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(CreateAclsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(CreateAclsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(CreateAclsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), CreateAclsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct CreateAclsShardOwner {
    shared: Arc<CreateAclsShardState>,
}

impl CreateAclsShardOwner {
    pub(crate) fn new<W>(host: CreateAclsHost, wake: Arc<W>) -> Self
    where
        W: CreateAclsShardWake,
    {
        Self {
            shared: Arc::new(CreateAclsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> CreateAclsAdmissionPort {
        CreateAclsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, CreateAclsHost>, CreateAclsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(CreateAclsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(CreateAclsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut CreateAclsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, CreateAclsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl CreateAclsShardState {
    fn host(&self) -> Result<MutexGuard<'_, CreateAclsHost>, CreateAclsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| CreateAclsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsShardLockError {
    Contended,
    Poisoned,
}
