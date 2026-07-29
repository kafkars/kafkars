//! Linear synchronized ownership of one client-metrics resource host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::Moment;

use crate::clock::OperationDeadline;

use super::{
    ListClientMetricsResourcesAdmissionErrorKind, ListClientMetricsResourcesHost,
    ListClientMetricsResourcesHostError, host::ListClientMetricsResourcesAdmission,
};

pub(crate) trait ListClientMetricsResourcesShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListClientMetricsResourcesShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListClientMetricsResourcesShardWakeError {
    source: io::Error,
}

impl ListClientMetricsResourcesShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListClientMetricsResourcesShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListClientMetricsResources shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListClientMetricsResourcesShardWakeError {}

struct ListClientMetricsResourcesShardState {
    host: Mutex<ListClientMetricsResourcesHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListClientMetricsResourcesShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListClientMetricsResourcesAdmissionPort {
    shared: Arc<ListClientMetricsResourcesShardState>,
}

impl ListClientMetricsResourcesAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<ListClientMetricsResourcesAdmission, ListClientMetricsResourcesAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListClientMetricsResourcesAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListClientMetricsResourcesAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListClientMetricsResourcesAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListClientMetricsResourcesHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListClientMetricsResourcesShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListClientMetricsResourcesShardOwner {
    shared: Arc<ListClientMetricsResourcesShardState>,
}

impl ListClientMetricsResourcesShardOwner {
    pub(crate) fn new<W>(host: ListClientMetricsResourcesHost, wake: Arc<W>) -> Self
    where
        W: ListClientMetricsResourcesShardWake,
    {
        Self {
            shared: Arc::new(ListClientMetricsResourcesShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListClientMetricsResourcesAdmissionPort {
        ListClientMetricsResourcesAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, ListClientMetricsResourcesHost>,
        ListClientMetricsResourcesShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(ListClientMetricsResourcesShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(ListClientMetricsResourcesShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListClientMetricsResourcesHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListClientMetricsResourcesHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListClientMetricsResourcesShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, ListClientMetricsResourcesHost>,
        ListClientMetricsResourcesShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_| ListClientMetricsResourcesShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListClientMetricsResourcesShardLockError {
    Contended,
    Poisoned,
}
