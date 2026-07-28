//! Linear synchronized ownership of one Admin `DeleteRecords` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteRecordsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteRecordsAdmissionErrorKind, DeleteRecordsHost, DeleteRecordsHostError,
    host::DeleteRecordsAdmission,
};

pub(crate) trait DeleteRecordsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteRecordsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteRecordsShardWakeError {
    source: io::Error,
}

impl DeleteRecordsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteRecordsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteRecords shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DeleteRecordsShardWakeError {}

struct DeleteRecordsShardState {
    host: Mutex<DeleteRecordsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteRecordsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteRecordsAdmissionPort {
    shared: Arc<DeleteRecordsShardState>,
}

impl DeleteRecordsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteRecordsPlan,
    ) -> Result<DeleteRecordsAdmission, DeleteRecordsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteRecordsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteRecordsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteRecordsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(DeleteRecordsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteRecordsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteRecordsShardOwner {
    shared: Arc<DeleteRecordsShardState>,
}

impl DeleteRecordsShardOwner {
    pub(crate) fn new<W>(host: DeleteRecordsHost, wake: Arc<W>) -> Self
    where
        W: DeleteRecordsShardWake,
    {
        Self {
            shared: Arc::new(DeleteRecordsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteRecordsAdmissionPort {
        DeleteRecordsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteRecordsHost>, DeleteRecordsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DeleteRecordsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DeleteRecordsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteRecordsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteRecordsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteRecordsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DeleteRecordsHost>, DeleteRecordsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DeleteRecordsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteRecordsShardLockError {
    Contended,
    Poisoned,
}
