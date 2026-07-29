//! Linear synchronized ownership of one streams-group description host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeStreamsGroupPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeStreamsGroupAdmissionErrorKind, DescribeStreamsGroupHost,
    DescribeStreamsGroupHostError, host::DescribeStreamsGroupAdmission,
};

pub(crate) trait DescribeStreamsGroupShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeStreamsGroupShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeStreamsGroupShardWakeError {
    source: io::Error,
}

impl DescribeStreamsGroupShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeStreamsGroupShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeStreamsGroup shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeStreamsGroupShardWakeError {}

struct DescribeStreamsGroupShardState {
    host: Mutex<DescribeStreamsGroupHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeStreamsGroupShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeStreamsGroupAdmissionPort {
    shared: Arc<DescribeStreamsGroupShardState>,
}

impl DescribeStreamsGroupAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeStreamsGroupPlan,
    ) -> Result<DescribeStreamsGroupAdmission, DescribeStreamsGroupAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeStreamsGroupAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeStreamsGroupAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeStreamsGroupAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeStreamsGroupHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeStreamsGroupShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeStreamsGroupShardOwner {
    shared: Arc<DescribeStreamsGroupShardState>,
}

impl DescribeStreamsGroupShardOwner {
    pub(crate) fn new<W>(host: DescribeStreamsGroupHost, wake: Arc<W>) -> Self
    where
        W: DescribeStreamsGroupShardWake,
    {
        Self {
            shared: Arc::new(DescribeStreamsGroupShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeStreamsGroupAdmissionPort {
        DescribeStreamsGroupAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeStreamsGroupHost>, DescribeStreamsGroupShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeStreamsGroupShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeStreamsGroupShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeStreamsGroupHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeStreamsGroupHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeStreamsGroupShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeStreamsGroupHost>, DescribeStreamsGroupShardLockError> {
        self.host
            .lock()
            .map_err(|_| DescribeStreamsGroupShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupShardLockError {
    Contended,
    Poisoned,
}
