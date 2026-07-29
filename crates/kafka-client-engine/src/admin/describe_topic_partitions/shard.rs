//! Linear synchronized ownership of one topic-partition page host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeTopicPartitionsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminDescribeTopicPartitionsAdmissionErrorKind, AdminDescribeTopicPartitionsHost,
    AdminDescribeTopicPartitionsHostError, host::AdminDescribeTopicPartitionsAdmission,
};

pub(crate) trait AdminDescribeTopicPartitionsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminDescribeTopicPartitionsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminDescribeTopicPartitionsShardWakeError {
    source: io::Error,
}

impl AdminDescribeTopicPartitionsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminDescribeTopicPartitionsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTopicPartitions shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminDescribeTopicPartitionsShardWakeError {}

struct AdminDescribeTopicPartitionsShardState {
    host: Mutex<AdminDescribeTopicPartitionsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminDescribeTopicPartitionsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminDescribeTopicPartitionsAdmissionPort {
    shared: Arc<AdminDescribeTopicPartitionsShardState>,
}

impl AdminDescribeTopicPartitionsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeTopicPartitionsPlan,
    ) -> Result<AdminDescribeTopicPartitionsAdmission, AdminDescribeTopicPartitionsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminDescribeTopicPartitionsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminDescribeTopicPartitionsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminDescribeTopicPartitionsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminDescribeTopicPartitionsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminDescribeTopicPartitionsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminDescribeTopicPartitionsShardOwner {
    shared: Arc<AdminDescribeTopicPartitionsShardState>,
}

impl AdminDescribeTopicPartitionsShardOwner {
    pub(crate) fn new<W>(host: AdminDescribeTopicPartitionsHost, wake: Arc<W>) -> Self
    where
        W: AdminDescribeTopicPartitionsShardWake,
    {
        Self {
            shared: Arc::new(AdminDescribeTopicPartitionsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminDescribeTopicPartitionsAdmissionPort {
        AdminDescribeTopicPartitionsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AdminDescribeTopicPartitionsHost>,
        AdminDescribeTopicPartitionsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AdminDescribeTopicPartitionsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AdminDescribeTopicPartitionsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminDescribeTopicPartitionsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminDescribeTopicPartitionsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminDescribeTopicPartitionsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AdminDescribeTopicPartitionsHost>,
        AdminDescribeTopicPartitionsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_| AdminDescribeTopicPartitionsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeTopicPartitionsShardLockError {
    Contended,
    Poisoned,
}
