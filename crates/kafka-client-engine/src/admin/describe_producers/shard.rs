//! Linear synchronized ownership of one Admin `DescribeProducers` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminDescribeProducersPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminDescribeProducersAdmissionErrorKind, AdminDescribeProducersHost,
    AdminDescribeProducersHostError, host::AdminDescribeProducersAdmission,
};

pub(crate) trait AdminDescribeProducersShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminDescribeProducersShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminDescribeProducersShardWakeError {
    source: io::Error,
}

impl AdminDescribeProducersShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminDescribeProducersShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeProducers shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminDescribeProducersShardWakeError {}

struct AdminDescribeProducersShardState {
    host: Mutex<AdminDescribeProducersHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminDescribeProducersShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminDescribeProducersAdmissionPort {
    shared: Arc<AdminDescribeProducersShardState>,
}

impl AdminDescribeProducersAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeProducersPlan,
    ) -> Result<AdminDescribeProducersAdmission, AdminDescribeProducersAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminDescribeProducersAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminDescribeProducersAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminDescribeProducersAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminDescribeProducersHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminDescribeProducersShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminDescribeProducersShardOwner {
    shared: Arc<AdminDescribeProducersShardState>,
}

impl AdminDescribeProducersShardOwner {
    pub(crate) fn new<W>(host: AdminDescribeProducersHost, wake: Arc<W>) -> Self
    where
        W: AdminDescribeProducersShardWake,
    {
        Self {
            shared: Arc::new(AdminDescribeProducersShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminDescribeProducersAdmissionPort {
        AdminDescribeProducersAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AdminDescribeProducersHost>, AdminDescribeProducersShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AdminDescribeProducersShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AdminDescribeProducersShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminDescribeProducersHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminDescribeProducersHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminDescribeProducersShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AdminDescribeProducersHost>, AdminDescribeProducersShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| AdminDescribeProducersShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeProducersShardLockError {
    Contended,
    Poisoned,
}
