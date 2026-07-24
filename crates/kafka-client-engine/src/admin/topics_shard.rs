//! Linear synchronized ownership of one bounded `DescribeTopics` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeTopicsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeTopicsAdmissionErrorKind, DescribeTopicsHost, DescribeTopicsHostError,
    topics_host::DescribeTopicsAdmission,
};

pub(crate) trait DescribeTopicsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeTopicsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeTopicsShardWakeError {
    source: io::Error,
}

impl DescribeTopicsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeTopicsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeTopics shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeTopicsShardWakeError {}

struct DescribeTopicsShardState {
    host: Mutex<DescribeTopicsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeTopicsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeTopicsAdmissionPort {
    shared: Arc<DescribeTopicsShardState>,
}

impl DescribeTopicsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeTopicsPlan,
        retained_bytes: usize,
    ) -> Result<DescribeTopicsAdmission, DescribeTopicsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeTopicsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeTopicsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeTopicsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retained_bytes)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(DescribeTopicsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeTopicsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeTopicsShardOwner {
    shared: Arc<DescribeTopicsShardState>,
}

impl DescribeTopicsShardOwner {
    pub(crate) fn new<W>(host: DescribeTopicsHost, wake: Arc<W>) -> Self
    where
        W: DescribeTopicsShardWake,
    {
        Self {
            shared: Arc::new(DescribeTopicsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeTopicsAdmissionPort {
        DescribeTopicsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeTopicsHost>, DescribeTopicsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeTopicsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeTopicsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeTopicsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeTopicsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeTopicsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeTopicsHost>, DescribeTopicsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeTopicsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeTopicsShardLockError {
    Contended,
    Poisoned,
}
