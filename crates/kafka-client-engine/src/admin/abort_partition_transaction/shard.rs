//! Linear synchronized ownership of one partition transaction-abort host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AbortPartitionTransactionPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AbortPartitionTransactionAdmissionErrorKind, AbortPartitionTransactionHost,
    AbortPartitionTransactionHostError, host::AbortPartitionTransactionAdmission,
};

pub(crate) trait AbortPartitionTransactionShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AbortPartitionTransactionShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AbortPartitionTransactionShardWakeError {
    source: io::Error,
}

impl AbortPartitionTransactionShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AbortPartitionTransactionShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "partition transaction-abort shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AbortPartitionTransactionShardWakeError {}

struct AbortPartitionTransactionShardState {
    host: Mutex<AbortPartitionTransactionHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AbortPartitionTransactionShardWake>,
}

#[derive(Clone)]
pub(crate) struct AbortPartitionTransactionAdmissionPort {
    shared: Arc<AbortPartitionTransactionShardState>,
}

impl AbortPartitionTransactionAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AbortPartitionTransactionPlan,
    ) -> Result<AbortPartitionTransactionAdmission, AbortPartitionTransactionAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AbortPartitionTransactionAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AbortPartitionTransactionAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AbortPartitionTransactionAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AbortPartitionTransactionHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AbortPartitionTransactionShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AbortPartitionTransactionShardOwner {
    shared: Arc<AbortPartitionTransactionShardState>,
}

impl AbortPartitionTransactionShardOwner {
    pub(crate) fn new<W>(host: AbortPartitionTransactionHost, wake: Arc<W>) -> Self
    where
        W: AbortPartitionTransactionShardWake,
    {
        Self {
            shared: Arc::new(AbortPartitionTransactionShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AbortPartitionTransactionAdmissionPort {
        AbortPartitionTransactionAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AbortPartitionTransactionHost>,
        AbortPartitionTransactionShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AbortPartitionTransactionShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AbortPartitionTransactionShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AbortPartitionTransactionHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AbortPartitionTransactionHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AbortPartitionTransactionShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AbortPartitionTransactionHost>,
        AbortPartitionTransactionShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_| AbortPartitionTransactionShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPartitionTransactionShardLockError {
    Contended,
    Poisoned,
}
