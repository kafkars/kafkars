//! Linear synchronized ownership of one group-description host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminDescribeConsumerGroupsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeConsumerGroupsAdmissionErrorKind, DescribeConsumerGroupsHost,
    DescribeConsumerGroupsHostError, host::DescribeConsumerGroupsAdmission,
};

pub(crate) trait DescribeConsumerGroupsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeConsumerGroupsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeConsumerGroupsShardWakeError {
    source: io::Error,
}

impl DescribeConsumerGroupsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeConsumerGroupsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConsumerGroups shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeConsumerGroupsShardWakeError {}

struct DescribeConsumerGroupsShardState {
    host: Mutex<DescribeConsumerGroupsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeConsumerGroupsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeConsumerGroupsAdmissionPort {
    shared: Arc<DescribeConsumerGroupsShardState>,
}

impl DescribeConsumerGroupsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeConsumerGroupsPlan,
    ) -> Result<DescribeConsumerGroupsAdmission, DescribeConsumerGroupsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeConsumerGroupsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeConsumerGroupsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeConsumerGroupsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeConsumerGroupsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeConsumerGroupsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeConsumerGroupsShardOwner {
    shared: Arc<DescribeConsumerGroupsShardState>,
}

impl DescribeConsumerGroupsShardOwner {
    pub(crate) fn new<W>(host: DescribeConsumerGroupsHost, wake: Arc<W>) -> Self
    where
        W: DescribeConsumerGroupsShardWake,
    {
        Self {
            shared: Arc::new(DescribeConsumerGroupsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeConsumerGroupsAdmissionPort {
        DescribeConsumerGroupsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeConsumerGroupsHost>, DescribeConsumerGroupsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeConsumerGroupsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeConsumerGroupsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeConsumerGroupsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeConsumerGroupsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeConsumerGroupsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeConsumerGroupsHost>, DescribeConsumerGroupsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_| DescribeConsumerGroupsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConsumerGroupsShardLockError {
    Contended,
    Poisoned,
}
