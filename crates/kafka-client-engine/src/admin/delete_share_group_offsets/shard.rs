//! Linear synchronized ownership of one share-group offset deletion host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteShareGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteShareGroupOffsetsAdmissionErrorKind, DeleteShareGroupOffsetsHost,
    DeleteShareGroupOffsetsHostError, host::DeleteShareGroupOffsetsAdmission,
};

pub(crate) trait DeleteShareGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteShareGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteShareGroupOffsetsShardWakeError {
    source: io::Error,
}

impl DeleteShareGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteShareGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteShareGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DeleteShareGroupOffsetsShardWakeError {}

struct DeleteShareGroupOffsetsShardState {
    host: Mutex<DeleteShareGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteShareGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteShareGroupOffsetsAdmissionPort {
    shared: Arc<DeleteShareGroupOffsetsShardState>,
}

impl DeleteShareGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteShareGroupOffsetsPlan,
    ) -> Result<DeleteShareGroupOffsetsAdmission, DeleteShareGroupOffsetsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteShareGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteShareGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DeleteShareGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteShareGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteShareGroupOffsetsShardOwner {
    shared: Arc<DeleteShareGroupOffsetsShardState>,
}

impl DeleteShareGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: DeleteShareGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: DeleteShareGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(DeleteShareGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteShareGroupOffsetsAdmissionPort {
        DeleteShareGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteShareGroupOffsetsHost>, DeleteShareGroupOffsetsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DeleteShareGroupOffsetsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DeleteShareGroupOffsetsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteShareGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteShareGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteShareGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteShareGroupOffsetsHost>, DeleteShareGroupOffsetsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_| DeleteShareGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteShareGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
