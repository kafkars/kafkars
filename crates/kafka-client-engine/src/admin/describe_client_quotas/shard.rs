//! Linear synchronized ownership of one Admin `DescribeClientQuotas` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeClientQuotasPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeClientQuotasAdmissionErrorKind, DescribeClientQuotasHost,
    DescribeClientQuotasHostError, host::DescribeClientQuotasAdmission,
};

pub(crate) trait DescribeClientQuotasShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeClientQuotasShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeClientQuotasShardWakeError {
    source: io::Error,
}

impl DescribeClientQuotasShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeClientQuotasShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeClientQuotas shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeClientQuotasShardWakeError {}

struct DescribeClientQuotasShardState {
    host: Mutex<DescribeClientQuotasHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeClientQuotasShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeClientQuotasAdmissionPort {
    shared: Arc<DescribeClientQuotasShardState>,
}

impl DescribeClientQuotasAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeClientQuotasPlan,
    ) -> Result<DescribeClientQuotasAdmission, DescribeClientQuotasAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeClientQuotasAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeClientQuotasAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeClientQuotasAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeClientQuotasHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeClientQuotasShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeClientQuotasShardOwner {
    shared: Arc<DescribeClientQuotasShardState>,
}

impl DescribeClientQuotasShardOwner {
    pub(crate) fn new<W>(host: DescribeClientQuotasHost, wake: Arc<W>) -> Self
    where
        W: DescribeClientQuotasShardWake,
    {
        Self {
            shared: Arc::new(DescribeClientQuotasShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeClientQuotasAdmissionPort {
        DescribeClientQuotasAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeClientQuotasHost>, DescribeClientQuotasShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeClientQuotasShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeClientQuotasShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeClientQuotasHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeClientQuotasHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeClientQuotasShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeClientQuotasHost>, DescribeClientQuotasShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeClientQuotasShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasShardLockError {
    Contended,
    Poisoned,
}
