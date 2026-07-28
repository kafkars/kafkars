//! Linear synchronized ownership of one Admin `DeleteConsumerGroups` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DeleteConsumerGroupsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DeleteConsumerGroupsAdmissionErrorKind, DeleteConsumerGroupsHost,
    DeleteConsumerGroupsHostError, host::DeleteConsumerGroupsAdmission,
};

pub(crate) trait DeleteConsumerGroupsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DeleteConsumerGroupsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DeleteConsumerGroupsShardWakeError {
    source: io::Error,
}

impl DeleteConsumerGroupsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DeleteConsumerGroupsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DeleteConsumerGroups shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DeleteConsumerGroupsShardWakeError {}

struct DeleteConsumerGroupsShardState {
    host: Mutex<DeleteConsumerGroupsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DeleteConsumerGroupsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DeleteConsumerGroupsAdmissionPort {
    shared: Arc<DeleteConsumerGroupsShardState>,
}

impl DeleteConsumerGroupsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DeleteConsumerGroupsPlan,
    ) -> Result<DeleteConsumerGroupsAdmission, DeleteConsumerGroupsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DeleteConsumerGroupsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DeleteConsumerGroupsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DeleteConsumerGroupsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DeleteConsumerGroupsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DeleteConsumerGroupsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DeleteConsumerGroupsShardOwner {
    shared: Arc<DeleteConsumerGroupsShardState>,
}

impl DeleteConsumerGroupsShardOwner {
    pub(crate) fn new<W>(host: DeleteConsumerGroupsHost, wake: Arc<W>) -> Self
    where
        W: DeleteConsumerGroupsShardWake,
    {
        Self {
            shared: Arc::new(DeleteConsumerGroupsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DeleteConsumerGroupsAdmissionPort {
        DeleteConsumerGroupsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteConsumerGroupsHost>, DeleteConsumerGroupsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DeleteConsumerGroupsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DeleteConsumerGroupsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DeleteConsumerGroupsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DeleteConsumerGroupsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DeleteConsumerGroupsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DeleteConsumerGroupsHost>, DeleteConsumerGroupsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DeleteConsumerGroupsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupsShardLockError {
    Contended,
    Poisoned,
}
