//! Linear synchronized ownership of one bounded `DescribeCluster` host.

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
    DescribeClusterAdmissionErrorKind, DescribeClusterHost, DescribeClusterHostError,
    describe_host::DescribeClusterAdmission,
};

pub(crate) trait DescribeClusterShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeClusterShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeClusterShardWakeError {
    source: io::Error,
}

impl DescribeClusterShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeClusterShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeCluster shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeClusterShardWakeError {}

struct DescribeClusterShardState {
    host: Mutex<DescribeClusterHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeClusterShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeClusterAdmissionPort {
    shared: Arc<DescribeClusterShardState>,
}

impl DescribeClusterAdmissionPort {
    #[cfg(test)]
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeClusterAdmission, DescribeClusterAdmissionErrorKind> {
        self.try_admit_with_options(now, deadline, false, false)
    }

    pub(crate) fn try_admit_with_options(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
    ) -> Result<DescribeClusterAdmission, DescribeClusterAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeClusterAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeClusterAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeClusterAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit_with_options(
            now,
            deadline,
            include_fenced_brokers,
            include_authorized_operations,
        )?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeClusterHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeClusterShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeClusterShardOwner {
    shared: Arc<DescribeClusterShardState>,
}

impl DescribeClusterShardOwner {
    pub(crate) fn new<W>(host: DescribeClusterHost, wake: Arc<W>) -> Self
    where
        W: DescribeClusterShardWake,
    {
        Self {
            shared: Arc::new(DescribeClusterShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeClusterAdmissionPort {
        DescribeClusterAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeClusterHost>, DescribeClusterShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeClusterShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeClusterShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeClusterHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeClusterHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeClusterShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeClusterHost>, DescribeClusterShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeClusterShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClusterShardLockError {
    Contended,
    Poisoned,
}
