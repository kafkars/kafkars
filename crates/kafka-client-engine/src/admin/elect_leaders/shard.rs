//! Synchronized linear ownership of one election host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ElectLeadersPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    ElectLeadersAdmissionErrorKind, ElectLeadersHost, ElectLeadersHostError,
    host::ElectLeadersAdmission,
};

pub(crate) trait ElectLeadersShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ElectLeadersShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ElectLeadersShardWakeError {
    source: io::Error,
}

impl ElectLeadersShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ElectLeadersShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "election shard wake failed: {}", self.source)
    }
}

impl std::error::Error for ElectLeadersShardWakeError {}

struct ElectLeadersShardState {
    host: Mutex<ElectLeadersHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ElectLeadersShardWake>,
}

#[derive(Clone)]
pub(crate) struct ElectLeadersAdmissionPort {
    shared: Arc<ElectLeadersShardState>,
}

impl ElectLeadersAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ElectLeadersPlan,
    ) -> Result<ElectLeadersAdmission, ElectLeadersAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ElectLeadersAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ElectLeadersAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ElectLeadersAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(ElectLeadersHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ElectLeadersShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ElectLeadersShardOwner {
    shared: Arc<ElectLeadersShardState>,
}

impl ElectLeadersShardOwner {
    pub(crate) fn new<W>(host: ElectLeadersHost, wake: Arc<W>) -> Self
    where
        W: ElectLeadersShardWake,
    {
        Self {
            shared: Arc::new(ElectLeadersShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ElectLeadersAdmissionPort {
        ElectLeadersAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ElectLeadersHost>, ElectLeadersShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ElectLeadersShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ElectLeadersShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ElectLeadersHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ElectLeadersHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ElectLeadersShardState {
    fn host(&self) -> Result<MutexGuard<'_, ElectLeadersHost>, ElectLeadersShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| ElectLeadersShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersShardLockError {
    Contended,
    Poisoned,
}
