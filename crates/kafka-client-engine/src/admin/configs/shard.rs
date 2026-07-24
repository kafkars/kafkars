//! Linear synchronized ownership for one bounded `DescribeConfigs` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeConfigsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeConfigsAdmissionErrorKind, DescribeConfigsHost, DescribeConfigsHostError,
    DescribeConfigsRetention, host::DescribeConfigsAdmission, model::topic_plan_supported,
};

pub(crate) trait DescribeConfigsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeConfigsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeConfigsShardWakeError {
    source: io::Error,
}

impl DescribeConfigsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeConfigsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeConfigs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeConfigsShardWakeError {}

struct DescribeConfigsShardState {
    host: Mutex<DescribeConfigsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeConfigsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeConfigsAdmissionPort {
    shared: Arc<DescribeConfigsShardState>,
}

impl DescribeConfigsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeConfigsPlan,
        retention: DescribeConfigsRetention,
    ) -> Result<DescribeConfigsAdmission, DescribeConfigsAdmissionErrorKind> {
        if !topic_plan_supported(&plan) {
            return Err(DescribeConfigsAdmissionErrorKind::UnsupportedResource);
        }
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeConfigsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeConfigsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeConfigsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retention)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeConfigsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeConfigsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeConfigsShardOwner {
    shared: Arc<DescribeConfigsShardState>,
}

impl DescribeConfigsShardOwner {
    pub(crate) fn new<W>(host: DescribeConfigsHost, wake: Arc<W>) -> Self
    where
        W: DescribeConfigsShardWake,
    {
        Self {
            shared: Arc::new(DescribeConfigsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeConfigsAdmissionPort {
        DescribeConfigsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeConfigsHost>, DescribeConfigsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeConfigsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeConfigsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeConfigsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeConfigsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeConfigsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeConfigsHost>, DescribeConfigsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeConfigsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeConfigsShardLockError {
    Contended,
    Poisoned,
}
