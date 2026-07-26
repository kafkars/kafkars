//! Linear synchronized ownership of one offset-deletion host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteConsumerGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteConsumerGroupOffsetsAdmissionErrorKind, DeleteConsumerGroupOffsetsHost,
    DeleteConsumerGroupOffsetsHostError, host::DeleteConsumerGroupOffsetsAdmission,
};

pub(crate) trait DeleteConsumerGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteConsumerGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteConsumerGroupOffsetsShardWakeError {
    source: io::Error,
}

impl DeleteConsumerGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteConsumerGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeleteConsumerGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DeleteConsumerGroupOffsetsShardWakeError {}

struct DeleteConsumerGroupOffsetsShardState {
    host: Mutex<DeleteConsumerGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteConsumerGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteConsumerGroupOffsetsAdmissionPort {
    shared: Arc<DeleteConsumerGroupOffsetsShardState>,
}

impl DeleteConsumerGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteConsumerGroupOffsetsPlan,
    ) -> Result<DeleteConsumerGroupOffsetsAdmission, DeleteConsumerGroupOffsetsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DeleteConsumerGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteConsumerGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteConsumerGroupOffsetsShardOwner {
    shared: Arc<DeleteConsumerGroupOffsetsShardState>,
}

impl DeleteConsumerGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: DeleteConsumerGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: DeleteConsumerGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(DeleteConsumerGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteConsumerGroupOffsetsAdmissionPort {
        DeleteConsumerGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, DeleteConsumerGroupOffsetsHost>,
        DeleteConsumerGroupOffsetsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(DeleteConsumerGroupOffsetsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(DeleteConsumerGroupOffsetsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteConsumerGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteConsumerGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteConsumerGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, DeleteConsumerGroupOffsetsHost>,
        DeleteConsumerGroupOffsetsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| DeleteConsumerGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
