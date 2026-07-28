//! Linear synchronized ownership of one Admin `AlterClientQuotas` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterClientQuotasPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AlterClientQuotasAdmissionErrorKind, AlterClientQuotasHost, AlterClientQuotasHostError,
    host::AlterClientQuotasAdmission,
};

pub(crate) trait AlterClientQuotasShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterClientQuotasShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterClientQuotasShardWakeError {
    source: io::Error,
}

impl AlterClientQuotasShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterClientQuotasShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterClientQuotas shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AlterClientQuotasShardWakeError {}

struct AlterClientQuotasShardState {
    host: Mutex<AlterClientQuotasHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterClientQuotasShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterClientQuotasAdmissionPort {
    shared: Arc<AlterClientQuotasShardState>,
}

impl AlterClientQuotasAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterClientQuotasPlan,
    ) -> Result<AlterClientQuotasAdmission, AlterClientQuotasAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterClientQuotasAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterClientQuotasAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterClientQuotasAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterClientQuotasHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterClientQuotasShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterClientQuotasShardOwner {
    shared: Arc<AlterClientQuotasShardState>,
}

impl AlterClientQuotasShardOwner {
    pub(crate) fn new<W>(host: AlterClientQuotasHost, wake: Arc<W>) -> Self
    where
        W: AlterClientQuotasShardWake,
    {
        Self {
            shared: Arc::new(AlterClientQuotasShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterClientQuotasAdmissionPort {
        AlterClientQuotasAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AlterClientQuotasHost>, AlterClientQuotasShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AlterClientQuotasShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AlterClientQuotasShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterClientQuotasHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterClientQuotasHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterClientQuotasShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AlterClientQuotasHost>, AlterClientQuotasShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| AlterClientQuotasShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterClientQuotasShardLockError {
    Contended,
    Poisoned,
}
