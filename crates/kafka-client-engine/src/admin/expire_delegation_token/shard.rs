//! Linear synchronized ownership of one delegation-token expiration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ExpireDelegationTokenPlan, Moment};

use crate::{
    clock::OperationDeadline,
    protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest,
};

use super::{
    ExpireDelegationTokenAdmissionErrorKind, ExpireDelegationTokenHost,
    ExpireDelegationTokenHostError, host::ExpireDelegationTokenAdmission,
};

pub(crate) trait ExpireDelegationTokenShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ExpireDelegationTokenShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ExpireDelegationTokenShardWakeError {
    source: io::Error,
}

impl ExpireDelegationTokenShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ExpireDelegationTokenShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ExpireDelegationToken shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ExpireDelegationTokenShardWakeError {}

struct ExpireDelegationTokenShardState {
    host: Mutex<ExpireDelegationTokenHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ExpireDelegationTokenShardWake>,
}

#[derive(Clone)]
pub(crate) struct ExpireDelegationTokenAdmissionPort {
    shared: Arc<ExpireDelegationTokenShardState>,
}

impl ExpireDelegationTokenAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ExpireDelegationTokenPlan,
        prepared_request: PreparedExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenAdmission, ExpireDelegationTokenAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ExpireDelegationTokenAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ExpireDelegationTokenAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ExpireDelegationTokenAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, prepared_request)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ExpireDelegationTokenHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ExpireDelegationTokenShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ExpireDelegationTokenShardOwner {
    shared: Arc<ExpireDelegationTokenShardState>,
}

impl ExpireDelegationTokenShardOwner {
    pub(crate) fn new<W>(host: ExpireDelegationTokenHost, wake: Arc<W>) -> Self
    where
        W: ExpireDelegationTokenShardWake,
    {
        Self {
            shared: Arc::new(ExpireDelegationTokenShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ExpireDelegationTokenAdmissionPort {
        ExpireDelegationTokenAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ExpireDelegationTokenHost>, ExpireDelegationTokenShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ExpireDelegationTokenShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ExpireDelegationTokenShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ExpireDelegationTokenHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ExpireDelegationTokenHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ExpireDelegationTokenShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, ExpireDelegationTokenHost>, ExpireDelegationTokenShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| ExpireDelegationTokenShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpireDelegationTokenShardLockError {
    Contended,
    Poisoned,
}
