//! Linear synchronized ownership of one bounded `CreateTopics` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{CreateTopicsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    CreateTopicsAdmissionErrorKind, CreateTopicsHost, CreateTopicsHostError,
    host::CreateTopicsAdmission,
};

pub(crate) trait CreateTopicsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), CreateTopicsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct CreateTopicsShardWakeError {
    source: io::Error,
}

impl CreateTopicsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for CreateTopicsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CreateTopics shard wake failed: {}", self.source)
    }
}

impl std::error::Error for CreateTopicsShardWakeError {}

struct CreateTopicsShardState {
    host: Mutex<CreateTopicsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn CreateTopicsShardWake>,
}

#[derive(Clone)]
pub(crate) struct CreateTopicsAdmissionPort {
    shared: Arc<CreateTopicsShardState>,
}

impl CreateTopicsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreateTopicsPlan,
        retained_bytes: usize,
    ) -> Result<CreateTopicsAdmission, CreateTopicsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(CreateTopicsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(CreateTopicsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(CreateTopicsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retained_bytes)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(CreateTopicsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), CreateTopicsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct CreateTopicsShardOwner {
    shared: Arc<CreateTopicsShardState>,
}

impl CreateTopicsShardOwner {
    pub(crate) fn new<W>(host: CreateTopicsHost, wake: Arc<W>) -> Self
    where
        W: CreateTopicsShardWake,
    {
        Self {
            shared: Arc::new(CreateTopicsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> CreateTopicsAdmissionPort {
        CreateTopicsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, CreateTopicsHost>, CreateTopicsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(CreateTopicsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(CreateTopicsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut CreateTopicsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, CreateTopicsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl CreateTopicsShardState {
    fn host(&self) -> Result<MutexGuard<'_, CreateTopicsHost>, CreateTopicsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| CreateTopicsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateTopicsShardLockError {
    Contended,
    Poisoned,
}
