//! Linear synchronized ownership of one delegation-token creation host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{CreateDelegationTokenPlan, Moment};

use crate::{
    clock::OperationDeadline,
    protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest,
};

use super::{
    CreateDelegationTokenAdmissionErrorKind, CreateDelegationTokenHost,
    CreateDelegationTokenHostError, host::CreateDelegationTokenAdmission,
};

pub(crate) trait CreateDelegationTokenShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), CreateDelegationTokenShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct CreateDelegationTokenShardWakeError {
    source: io::Error,
}

impl CreateDelegationTokenShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for CreateDelegationTokenShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CreateDelegationToken shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for CreateDelegationTokenShardWakeError {}

struct CreateDelegationTokenShardState {
    host: Mutex<CreateDelegationTokenHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn CreateDelegationTokenShardWake>,
}

#[derive(Clone)]
pub(crate) struct CreateDelegationTokenAdmissionPort {
    shared: Arc<CreateDelegationTokenShardState>,
}

impl CreateDelegationTokenAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: CreateDelegationTokenPlan,
        prepared_request: PreparedCreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenAdmission, CreateDelegationTokenAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(CreateDelegationTokenAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(CreateDelegationTokenAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(CreateDelegationTokenAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, prepared_request)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(CreateDelegationTokenHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), CreateDelegationTokenShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct CreateDelegationTokenShardOwner {
    shared: Arc<CreateDelegationTokenShardState>,
}

impl CreateDelegationTokenShardOwner {
    pub(crate) fn new<W>(host: CreateDelegationTokenHost, wake: Arc<W>) -> Self
    where
        W: CreateDelegationTokenShardWake,
    {
        Self {
            shared: Arc::new(CreateDelegationTokenShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> CreateDelegationTokenAdmissionPort {
        CreateDelegationTokenAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, CreateDelegationTokenHost>, CreateDelegationTokenShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(CreateDelegationTokenShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(CreateDelegationTokenShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut CreateDelegationTokenHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, CreateDelegationTokenHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl CreateDelegationTokenShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, CreateDelegationTokenHost>, CreateDelegationTokenShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| CreateDelegationTokenShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenShardLockError {
    Contended,
    Poisoned,
}
