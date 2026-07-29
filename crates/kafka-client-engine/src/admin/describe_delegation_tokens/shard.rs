//! Linear synchronized ownership of one delegation-token description host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeDelegationTokensPlan, Moment};

use crate::{
    clock::OperationDeadline,
    protocol::admin::describe_delegation_tokens::PreparedDescribeDelegationTokensRequest,
};

use super::{
    DescribeDelegationTokensAdmissionErrorKind, DescribeDelegationTokensHost,
    DescribeDelegationTokensHostError, host::DescribeDelegationTokensAdmission,
};

pub(crate) trait DescribeDelegationTokensShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeDelegationTokensShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeDelegationTokensShardWakeError {
    source: io::Error,
}

impl DescribeDelegationTokensShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeDelegationTokensShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeDelegationTokens shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeDelegationTokensShardWakeError {}

struct DescribeDelegationTokensShardState {
    host: Mutex<DescribeDelegationTokensHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeDelegationTokensShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeDelegationTokensAdmissionPort {
    shared: Arc<DescribeDelegationTokensShardState>,
}

impl DescribeDelegationTokensAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeDelegationTokensPlan,
        prepared_request: PreparedDescribeDelegationTokensRequest,
    ) -> Result<DescribeDelegationTokensAdmission, DescribeDelegationTokensAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeDelegationTokensAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeDelegationTokensAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeDelegationTokensAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, prepared_request)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeDelegationTokensHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeDelegationTokensShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeDelegationTokensShardOwner {
    shared: Arc<DescribeDelegationTokensShardState>,
}

impl DescribeDelegationTokensShardOwner {
    pub(crate) fn new<W>(host: DescribeDelegationTokensHost, wake: Arc<W>) -> Self
    where
        W: DescribeDelegationTokensShardWake,
    {
        Self {
            shared: Arc::new(DescribeDelegationTokensShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeDelegationTokensAdmissionPort {
        DescribeDelegationTokensAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeDelegationTokensHost>, DescribeDelegationTokensShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeDelegationTokensShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeDelegationTokensShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeDelegationTokensHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeDelegationTokensHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeDelegationTokensShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeDelegationTokensHost>, DescribeDelegationTokensShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeDelegationTokensShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensShardLockError {
    Contended,
    Poisoned,
}
