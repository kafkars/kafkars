//! Linear synchronized ownership of one Admin `DescribeAcls` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeAclsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeAclsAdmissionErrorKind, DescribeAclsHost, DescribeAclsHostError,
    host::DescribeAclsAdmission,
};

pub(crate) trait DescribeAclsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeAclsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeAclsShardWakeError {
    source: io::Error,
}

impl DescribeAclsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeAclsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DescribeAcls shard wake failed: {}", self.source)
    }
}

impl std::error::Error for DescribeAclsShardWakeError {}

struct DescribeAclsShardState {
    host: Mutex<DescribeAclsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeAclsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeAclsAdmissionPort {
    shared: Arc<DescribeAclsShardState>,
}

impl DescribeAclsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeAclsPlan,
    ) -> Result<DescribeAclsAdmission, DescribeAclsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeAclsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeAclsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeAclsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(DescribeAclsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeAclsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeAclsShardOwner {
    shared: Arc<DescribeAclsShardState>,
}

impl DescribeAclsShardOwner {
    pub(crate) fn new<W>(host: DescribeAclsHost, wake: Arc<W>) -> Self
    where
        W: DescribeAclsShardWake,
    {
        Self {
            shared: Arc::new(DescribeAclsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeAclsAdmissionPort {
        DescribeAclsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeAclsHost>, DescribeAclsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeAclsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeAclsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeAclsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeAclsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeAclsShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeAclsHost>, DescribeAclsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeAclsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeAclsShardLockError {
    Contended,
    Poisoned,
}
