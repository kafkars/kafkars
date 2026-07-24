//! Linear synchronized ownership of one bounded `DeleteTopics` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteTopicsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteTopicsAdmissionErrorKind, DeleteTopicsHost, DeleteTopicsHostError,
    delete_host::DeleteTopicsAdmission,
};

pub(crate) trait DeleteTopicsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteTopicsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteTopicsShardWakeError {
    source: io::Error,
}

impl DeleteTopicsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteTopicsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DeleteTopics shard wake failed: {}", self.source)
    }
}

impl std::error::Error for DeleteTopicsShardWakeError {}

struct DeleteTopicsShardState {
    host: Mutex<DeleteTopicsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteTopicsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteTopicsAdmissionPort {
    shared: Arc<DeleteTopicsShardState>,
}

impl DeleteTopicsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteTopicsPlan,
        retained_bytes: usize,
    ) -> Result<DeleteTopicsAdmission, DeleteTopicsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteTopicsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteTopicsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteTopicsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retained_bytes)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(DeleteTopicsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteTopicsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteTopicsShardOwner {
    shared: Arc<DeleteTopicsShardState>,
}

impl DeleteTopicsShardOwner {
    pub(crate) fn new<W>(host: DeleteTopicsHost, wake: Arc<W>) -> Self
    where
        W: DeleteTopicsShardWake,
    {
        Self {
            shared: Arc::new(DeleteTopicsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteTopicsAdmissionPort {
        DeleteTopicsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteTopicsHost>, DeleteTopicsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DeleteTopicsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DeleteTopicsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteTopicsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteTopicsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteTopicsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DeleteTopicsHost>, DeleteTopicsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DeleteTopicsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteTopicsShardLockError {
    Contended,
    Poisoned,
}
