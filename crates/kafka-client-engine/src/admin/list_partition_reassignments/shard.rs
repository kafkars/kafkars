//! Linear synchronized ownership of one reassignment-listing host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ListPartitionReassignmentsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    ListPartitionReassignmentsAdmissionErrorKind, ListPartitionReassignmentsHost,
    ListPartitionReassignmentsHostError, host::ListPartitionReassignmentsAdmission,
};

pub(crate) trait ListPartitionReassignmentsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListPartitionReassignmentsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListPartitionReassignmentsShardWakeError {
    source: io::Error,
}

impl ListPartitionReassignmentsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListPartitionReassignmentsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListPartitionReassignments shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListPartitionReassignmentsShardWakeError {}

struct ListPartitionReassignmentsShardState {
    host: Mutex<ListPartitionReassignmentsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListPartitionReassignmentsShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListPartitionReassignmentsAdmissionPort {
    shared: Arc<ListPartitionReassignmentsShardState>,
}

impl ListPartitionReassignmentsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListPartitionReassignmentsPlan,
    ) -> Result<ListPartitionReassignmentsAdmission, ListPartitionReassignmentsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListPartitionReassignmentsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListPartitionReassignmentsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListPartitionReassignmentsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListPartitionReassignmentsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListPartitionReassignmentsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListPartitionReassignmentsShardOwner {
    shared: Arc<ListPartitionReassignmentsShardState>,
}

impl ListPartitionReassignmentsShardOwner {
    pub(crate) fn new<W>(host: ListPartitionReassignmentsHost, wake: Arc<W>) -> Self
    where
        W: ListPartitionReassignmentsShardWake,
    {
        Self {
            shared: Arc::new(ListPartitionReassignmentsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListPartitionReassignmentsAdmissionPort {
        ListPartitionReassignmentsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, ListPartitionReassignmentsHost>,
        ListPartitionReassignmentsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(ListPartitionReassignmentsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(ListPartitionReassignmentsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListPartitionReassignmentsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListPartitionReassignmentsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListPartitionReassignmentsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, ListPartitionReassignmentsHost>,
        ListPartitionReassignmentsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| ListPartitionReassignmentsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListPartitionReassignmentsShardLockError {
    Contended,
    Poisoned,
}
