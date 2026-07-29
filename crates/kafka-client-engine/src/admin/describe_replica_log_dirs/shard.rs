//! Linear synchronized ownership of one Admin `DescribeReplicaLogDirs` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeReplicaLogDirsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeReplicaLogDirsAdmissionErrorKind, DescribeReplicaLogDirsHost,
    DescribeReplicaLogDirsHostError, host::DescribeReplicaLogDirsAdmission,
};

pub(crate) trait DescribeReplicaLogDirsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeReplicaLogDirsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeReplicaLogDirsShardWakeError {
    source: io::Error,
}

impl DescribeReplicaLogDirsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeReplicaLogDirsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeReplicaLogDirs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeReplicaLogDirsShardWakeError {}

struct DescribeReplicaLogDirsShardState {
    host: Mutex<DescribeReplicaLogDirsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeReplicaLogDirsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeReplicaLogDirsAdmissionPort {
    shared: Arc<DescribeReplicaLogDirsShardState>,
}

impl DescribeReplicaLogDirsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeReplicaLogDirsPlan,
    ) -> Result<DescribeReplicaLogDirsAdmission, DescribeReplicaLogDirsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeReplicaLogDirsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeReplicaLogDirsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeReplicaLogDirsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeReplicaLogDirsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeReplicaLogDirsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeReplicaLogDirsShardOwner {
    shared: Arc<DescribeReplicaLogDirsShardState>,
}

impl DescribeReplicaLogDirsShardOwner {
    pub(crate) fn new<W>(host: DescribeReplicaLogDirsHost, wake: Arc<W>) -> Self
    where
        W: DescribeReplicaLogDirsShardWake,
    {
        Self {
            shared: Arc::new(DescribeReplicaLogDirsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeReplicaLogDirsAdmissionPort {
        DescribeReplicaLogDirsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeReplicaLogDirsHost>, DescribeReplicaLogDirsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeReplicaLogDirsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeReplicaLogDirsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeReplicaLogDirsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeReplicaLogDirsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeReplicaLogDirsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeReplicaLogDirsHost>, DescribeReplicaLogDirsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeReplicaLogDirsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeReplicaLogDirsShardLockError {
    Contended,
    Poisoned,
}
