//! Linear synchronized ownership of one Admin `DescribeLogDirs` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminDescribeLogDirsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeLogDirsAdmissionErrorKind, DescribeLogDirsHost, DescribeLogDirsHostError,
    host::DescribeLogDirsAdmission,
};

pub(crate) trait DescribeLogDirsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeLogDirsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeLogDirsShardWakeError {
    source: io::Error,
}

impl DescribeLogDirsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeLogDirsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeLogDirs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeLogDirsShardWakeError {}

struct DescribeLogDirsShardState {
    host: Mutex<DescribeLogDirsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeLogDirsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeLogDirsAdmissionPort {
    shared: Arc<DescribeLogDirsShardState>,
}

impl DescribeLogDirsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeLogDirsPlan,
    ) -> Result<DescribeLogDirsAdmission, DescribeLogDirsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeLogDirsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeLogDirsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeLogDirsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeLogDirsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeLogDirsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeLogDirsShardOwner {
    shared: Arc<DescribeLogDirsShardState>,
}

impl DescribeLogDirsShardOwner {
    pub(crate) fn new<W>(host: DescribeLogDirsHost, wake: Arc<W>) -> Self
    where
        W: DescribeLogDirsShardWake,
    {
        Self {
            shared: Arc::new(DescribeLogDirsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeLogDirsAdmissionPort {
        DescribeLogDirsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeLogDirsHost>, DescribeLogDirsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeLogDirsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeLogDirsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeLogDirsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeLogDirsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeLogDirsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeLogDirsHost>, DescribeLogDirsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeLogDirsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsShardLockError {
    Contended,
    Poisoned,
}
