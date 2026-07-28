//! Linear synchronized ownership of one Admin `AlterReplicaLogDirs` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterReplicaLogDirsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AlterReplicaLogDirsAdmissionErrorKind, AlterReplicaLogDirsHost, AlterReplicaLogDirsHostError,
    host::AlterReplicaLogDirsAdmission,
};

pub(crate) trait AlterReplicaLogDirsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterReplicaLogDirsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterReplicaLogDirsShardWakeError {
    source: io::Error,
}

impl AlterReplicaLogDirsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterReplicaLogDirsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterReplicaLogDirs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AlterReplicaLogDirsShardWakeError {}

struct AlterReplicaLogDirsShardState {
    host: Mutex<AlterReplicaLogDirsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterReplicaLogDirsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterReplicaLogDirsAdmissionPort {
    shared: Arc<AlterReplicaLogDirsShardState>,
}

impl AlterReplicaLogDirsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterReplicaLogDirsPlan,
    ) -> Result<AlterReplicaLogDirsAdmission, AlterReplicaLogDirsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterReplicaLogDirsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterReplicaLogDirsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterReplicaLogDirsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterReplicaLogDirsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterReplicaLogDirsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterReplicaLogDirsShardOwner {
    shared: Arc<AlterReplicaLogDirsShardState>,
}

impl AlterReplicaLogDirsShardOwner {
    pub(crate) fn new<W>(host: AlterReplicaLogDirsHost, wake: Arc<W>) -> Self
    where
        W: AlterReplicaLogDirsShardWake,
    {
        Self {
            shared: Arc::new(AlterReplicaLogDirsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterReplicaLogDirsAdmissionPort {
        AlterReplicaLogDirsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AlterReplicaLogDirsHost>, AlterReplicaLogDirsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AlterReplicaLogDirsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AlterReplicaLogDirsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterReplicaLogDirsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterReplicaLogDirsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterReplicaLogDirsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AlterReplicaLogDirsHost>, AlterReplicaLogDirsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| AlterReplicaLogDirsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterReplicaLogDirsShardLockError {
    Contended,
    Poisoned,
}
