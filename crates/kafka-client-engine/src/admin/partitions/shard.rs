//! Linear synchronized ownership of one bounded `CreatePartitions` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{CreatePartitionsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    CreatePartitionsAdmissionErrorKind, CreatePartitionsHost, CreatePartitionsHostError,
    host::CreatePartitionsAdmission,
};

pub(crate) trait CreatePartitionsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), CreatePartitionsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct CreatePartitionsShardWakeError {
    source: io::Error,
}

impl CreatePartitionsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for CreatePartitionsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreatePartitions shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for CreatePartitionsShardWakeError {}

struct CreatePartitionsShardState {
    host: Mutex<CreatePartitionsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn CreatePartitionsShardWake>,
}

#[derive(Clone)]
pub(crate) struct CreatePartitionsAdmissionPort {
    shared: Arc<CreatePartitionsShardState>,
}

impl CreatePartitionsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreatePartitionsPlan,
        retained_bytes: usize,
    ) -> Result<CreatePartitionsAdmission, CreatePartitionsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(CreatePartitionsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(CreatePartitionsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(CreatePartitionsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retained_bytes)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(CreatePartitionsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), CreatePartitionsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct CreatePartitionsShardOwner {
    shared: Arc<CreatePartitionsShardState>,
}

impl CreatePartitionsShardOwner {
    pub(crate) fn new<W>(host: CreatePartitionsHost, wake: Arc<W>) -> Self
    where
        W: CreatePartitionsShardWake,
    {
        Self {
            shared: Arc::new(CreatePartitionsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> CreatePartitionsAdmissionPort {
        CreatePartitionsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, CreatePartitionsHost>, CreatePartitionsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(CreatePartitionsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(CreatePartitionsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut CreatePartitionsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, CreatePartitionsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl CreatePartitionsShardState {
    fn host(&self) -> Result<MutexGuard<'_, CreatePartitionsHost>, CreatePartitionsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| CreatePartitionsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreatePartitionsShardLockError {
    Contended,
    Poisoned,
}
