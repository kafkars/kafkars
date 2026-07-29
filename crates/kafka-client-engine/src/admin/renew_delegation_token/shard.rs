//! Linear synchronized ownership of one delegation-token renewal host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{Moment, RenewDelegationTokenPlan};

use crate::{
    clock::OperationDeadline,
    protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest,
};

use super::{
    RenewDelegationTokenAdmissionErrorKind, RenewDelegationTokenHost,
    RenewDelegationTokenHostError, host::RenewDelegationTokenAdmission,
};

pub(crate) trait RenewDelegationTokenShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), RenewDelegationTokenShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct RenewDelegationTokenShardWakeError {
    source: io::Error,
}

impl RenewDelegationTokenShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for RenewDelegationTokenShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RenewDelegationToken shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for RenewDelegationTokenShardWakeError {}

struct RenewDelegationTokenShardState {
    host: Mutex<RenewDelegationTokenHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn RenewDelegationTokenShardWake>,
}

#[derive(Clone)]
pub(crate) struct RenewDelegationTokenAdmissionPort {
    shared: Arc<RenewDelegationTokenShardState>,
}

impl RenewDelegationTokenAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RenewDelegationTokenPlan,
        prepared_request: PreparedRenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenAdmission, RenewDelegationTokenAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(RenewDelegationTokenAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(RenewDelegationTokenAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(RenewDelegationTokenAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, prepared_request)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(RenewDelegationTokenHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), RenewDelegationTokenShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct RenewDelegationTokenShardOwner {
    shared: Arc<RenewDelegationTokenShardState>,
}

impl RenewDelegationTokenShardOwner {
    pub(crate) fn new<W>(host: RenewDelegationTokenHost, wake: Arc<W>) -> Self
    where
        W: RenewDelegationTokenShardWake,
    {
        Self {
            shared: Arc::new(RenewDelegationTokenShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> RenewDelegationTokenAdmissionPort {
        RenewDelegationTokenAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, RenewDelegationTokenHost>, RenewDelegationTokenShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(RenewDelegationTokenShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(RenewDelegationTokenShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut RenewDelegationTokenHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, RenewDelegationTokenHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl RenewDelegationTokenShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, RenewDelegationTokenHost>, RenewDelegationTokenShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| RenewDelegationTokenShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RenewDelegationTokenShardLockError {
    Contended,
    Poisoned,
}
