//! Linear synchronized ownership of one Admin `FenceProducers` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminFenceProducersPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminFenceProducersAdmissionErrorKind, AdminFenceProducersHost, AdminFenceProducersHostError,
    host::AdminFenceProducersAdmission,
};

pub(crate) trait AdminFenceProducersShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminFenceProducersShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminFenceProducersShardWakeError {
    source: io::Error,
}

impl AdminFenceProducersShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminFenceProducersShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin FenceProducers shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminFenceProducersShardWakeError {}

struct AdminFenceProducersShardState {
    host: Mutex<AdminFenceProducersHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminFenceProducersShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminFenceProducersAdmissionPort {
    shared: Arc<AdminFenceProducersShardState>,
}

impl AdminFenceProducersAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminFenceProducersPlan,
    ) -> Result<AdminFenceProducersAdmission, AdminFenceProducersAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminFenceProducersAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminFenceProducersAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminFenceProducersAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminFenceProducersHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminFenceProducersShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminFenceProducersShardOwner {
    shared: Arc<AdminFenceProducersShardState>,
}

impl AdminFenceProducersShardOwner {
    pub(crate) fn new<W>(host: AdminFenceProducersHost, wake: Arc<W>) -> Self
    where
        W: AdminFenceProducersShardWake,
    {
        Self {
            shared: Arc::new(AdminFenceProducersShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminFenceProducersAdmissionPort {
        AdminFenceProducersAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AdminFenceProducersHost>, AdminFenceProducersShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AdminFenceProducersShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AdminFenceProducersShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminFenceProducersHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminFenceProducersHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminFenceProducersShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AdminFenceProducersHost>, AdminFenceProducersShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| AdminFenceProducersShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminFenceProducersShardLockError {
    Contended,
    Poisoned,
}
