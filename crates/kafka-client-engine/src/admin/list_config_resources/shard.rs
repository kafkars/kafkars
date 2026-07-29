//! Linear synchronized ownership of one configuration-resource listing host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ListConfigResourcesPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    ListConfigResourcesAdmissionErrorKind, ListConfigResourcesHost, ListConfigResourcesHostError,
    host::ListConfigResourcesAdmission,
};

pub(crate) trait ListConfigResourcesShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListConfigResourcesShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListConfigResourcesShardWakeError {
    source: io::Error,
}

impl ListConfigResourcesShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListConfigResourcesShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListConfigResources shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListConfigResourcesShardWakeError {}

struct ListConfigResourcesShardState {
    host: Mutex<ListConfigResourcesHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListConfigResourcesShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListConfigResourcesAdmissionPort {
    shared: Arc<ListConfigResourcesShardState>,
}

impl ListConfigResourcesAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListConfigResourcesPlan,
    ) -> Result<ListConfigResourcesAdmission, ListConfigResourcesAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListConfigResourcesAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListConfigResourcesAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListConfigResourcesAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListConfigResourcesHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListConfigResourcesShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListConfigResourcesShardOwner {
    shared: Arc<ListConfigResourcesShardState>,
}

impl ListConfigResourcesShardOwner {
    pub(crate) fn new<W>(host: ListConfigResourcesHost, wake: Arc<W>) -> Self
    where
        W: ListConfigResourcesShardWake,
    {
        Self {
            shared: Arc::new(ListConfigResourcesShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListConfigResourcesAdmissionPort {
        ListConfigResourcesAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ListConfigResourcesHost>, ListConfigResourcesShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ListConfigResourcesShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ListConfigResourcesShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListConfigResourcesHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListConfigResourcesHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListConfigResourcesShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, ListConfigResourcesHost>, ListConfigResourcesShardLockError> {
        self.host
            .lock()
            .map_err(|_| ListConfigResourcesShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConfigResourcesShardLockError {
    Contended,
    Poisoned,
}
