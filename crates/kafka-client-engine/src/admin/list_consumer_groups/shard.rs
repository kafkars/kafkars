//! Linear synchronized ownership of one cluster group-listing host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminGroupListingFilters, AdminGroupListingScope, Moment};

use crate::clock::OperationDeadline;

use super::{
    ListConsumerGroupsAdmissionErrorKind, ListConsumerGroupsHost, ListConsumerGroupsHostError,
    host::ListConsumerGroupsAdmission,
};

pub(crate) trait ListConsumerGroupsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListConsumerGroupsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListConsumerGroupsShardWakeError {
    source: io::Error,
}

impl ListConsumerGroupsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListConsumerGroupsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroups shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListConsumerGroupsShardWakeError {}

struct ListConsumerGroupsShardState {
    host: Mutex<ListConsumerGroupsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListConsumerGroupsShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListConsumerGroupsAdmissionPort {
    shared: Arc<ListConsumerGroupsShardState>,
}

impl ListConsumerGroupsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        scope: AdminGroupListingScope,
        filters: AdminGroupListingFilters,
    ) -> Result<ListConsumerGroupsAdmission, ListConsumerGroupsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListConsumerGroupsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListConsumerGroupsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListConsumerGroupsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, scope, filters)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListConsumerGroupsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListConsumerGroupsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListConsumerGroupsShardOwner {
    shared: Arc<ListConsumerGroupsShardState>,
}

impl ListConsumerGroupsShardOwner {
    pub(crate) fn new<W>(host: ListConsumerGroupsHost, wake: Arc<W>) -> Self
    where
        W: ListConsumerGroupsShardWake,
    {
        Self {
            shared: Arc::new(ListConsumerGroupsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListConsumerGroupsAdmissionPort {
        ListConsumerGroupsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ListConsumerGroupsHost>, ListConsumerGroupsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ListConsumerGroupsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ListConsumerGroupsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListConsumerGroupsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListConsumerGroupsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListConsumerGroupsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, ListConsumerGroupsHost>, ListConsumerGroupsShardLockError> {
        self.host
            .lock()
            .map_err(|_| ListConsumerGroupsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupsShardLockError {
    Contended,
    Poisoned,
}
