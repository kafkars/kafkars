//! Linear synchronized ownership of one share-group description host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeShareGroupPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeShareGroupAdmissionErrorKind, DescribeShareGroupHost, DescribeShareGroupHostError,
    host::DescribeShareGroupAdmission,
};

pub(crate) trait DescribeShareGroupShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeShareGroupShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeShareGroupShardWakeError {
    source: io::Error,
}

impl DescribeShareGroupShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeShareGroupShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeShareGroup shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeShareGroupShardWakeError {}

struct DescribeShareGroupShardState {
    host: Mutex<DescribeShareGroupHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeShareGroupShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeShareGroupAdmissionPort {
    shared: Arc<DescribeShareGroupShardState>,
}

impl DescribeShareGroupAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeShareGroupPlan,
    ) -> Result<DescribeShareGroupAdmission, DescribeShareGroupAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeShareGroupAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeShareGroupAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeShareGroupAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeShareGroupHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeShareGroupShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeShareGroupShardOwner {
    shared: Arc<DescribeShareGroupShardState>,
}

impl DescribeShareGroupShardOwner {
    pub(crate) fn new<W>(host: DescribeShareGroupHost, wake: Arc<W>) -> Self
    where
        W: DescribeShareGroupShardWake,
    {
        Self {
            shared: Arc::new(DescribeShareGroupShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeShareGroupAdmissionPort {
        DescribeShareGroupAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeShareGroupHost>, DescribeShareGroupShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeShareGroupShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeShareGroupShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeShareGroupHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeShareGroupHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeShareGroupShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeShareGroupHost>, DescribeShareGroupShardLockError> {
        self.host
            .lock()
            .map_err(|_| DescribeShareGroupShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupShardLockError {
    Contended,
    Poisoned,
}
