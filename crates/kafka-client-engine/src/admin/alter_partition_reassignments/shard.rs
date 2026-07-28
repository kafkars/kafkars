//! Synchronized linear ownership of one reassignment host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterPartitionReassignmentsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AlterPartitionReassignmentsAdmissionErrorKind, AlterPartitionReassignmentsHost,
    AlterPartitionReassignmentsHostError, host::AlterPartitionReassignmentsAdmission,
};

pub(crate) trait AlterPartitionReassignmentsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterPartitionReassignmentsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterPartitionReassignmentsShardWakeError {
    source: io::Error,
}

impl AlterPartitionReassignmentsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterPartitionReassignmentsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "reassignment shard wake failed: {}", self.source)
    }
}

impl std::error::Error for AlterPartitionReassignmentsShardWakeError {}

struct AlterPartitionReassignmentsShardState {
    host: Mutex<AlterPartitionReassignmentsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterPartitionReassignmentsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterPartitionReassignmentsAdmissionPort {
    shared: Arc<AlterPartitionReassignmentsShardState>,
}

impl AlterPartitionReassignmentsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterPartitionReassignmentsPlan,
    ) -> Result<AlterPartitionReassignmentsAdmission, AlterPartitionReassignmentsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterPartitionReassignmentsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterPartitionReassignmentsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterPartitionReassignmentsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterPartitionReassignmentsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterPartitionReassignmentsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterPartitionReassignmentsShardOwner {
    shared: Arc<AlterPartitionReassignmentsShardState>,
}

impl AlterPartitionReassignmentsShardOwner {
    pub(crate) fn new<W>(host: AlterPartitionReassignmentsHost, wake: Arc<W>) -> Self
    where
        W: AlterPartitionReassignmentsShardWake,
    {
        Self {
            shared: Arc::new(AlterPartitionReassignmentsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterPartitionReassignmentsAdmissionPort {
        AlterPartitionReassignmentsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterPartitionReassignmentsHost>,
        AlterPartitionReassignmentsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AlterPartitionReassignmentsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AlterPartitionReassignmentsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterPartitionReassignmentsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterPartitionReassignmentsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterPartitionReassignmentsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterPartitionReassignmentsHost>,
        AlterPartitionReassignmentsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| AlterPartitionReassignmentsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterPartitionReassignmentsShardLockError {
    Contended,
    Poisoned,
}
