//! Linear synchronized ownership of one Admin `DescribeUserScramCredentials` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{DescribeUserScramCredentialsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    DescribeUserScramCredentialsAdmissionErrorKind, DescribeUserScramCredentialsHost,
    DescribeUserScramCredentialsHostError, host::DescribeUserScramCredentialsAdmission,
};

pub(crate) trait DescribeUserScramCredentialsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeUserScramCredentialsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeUserScramCredentialsShardWakeError {
    source: io::Error,
}

impl DescribeUserScramCredentialsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeUserScramCredentialsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeUserScramCredentials shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeUserScramCredentialsShardWakeError {}

struct DescribeUserScramCredentialsShardState {
    host: Mutex<DescribeUserScramCredentialsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeUserScramCredentialsShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeUserScramCredentialsAdmissionPort {
    shared: Arc<DescribeUserScramCredentialsShardState>,
}

impl DescribeUserScramCredentialsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: DescribeUserScramCredentialsPlan,
    ) -> Result<DescribeUserScramCredentialsAdmission, DescribeUserScramCredentialsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeUserScramCredentialsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeUserScramCredentialsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeUserScramCredentialsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeUserScramCredentialsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeUserScramCredentialsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeUserScramCredentialsShardOwner {
    shared: Arc<DescribeUserScramCredentialsShardState>,
}

impl DescribeUserScramCredentialsShardOwner {
    pub(crate) fn new<W>(host: DescribeUserScramCredentialsHost, wake: Arc<W>) -> Self
    where
        W: DescribeUserScramCredentialsShardWake,
    {
        Self {
            shared: Arc::new(DescribeUserScramCredentialsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeUserScramCredentialsAdmissionPort {
        DescribeUserScramCredentialsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, DescribeUserScramCredentialsHost>,
        DescribeUserScramCredentialsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(DescribeUserScramCredentialsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(DescribeUserScramCredentialsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeUserScramCredentialsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeUserScramCredentialsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeUserScramCredentialsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, DescribeUserScramCredentialsHost>,
        DescribeUserScramCredentialsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeUserScramCredentialsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeUserScramCredentialsShardLockError {
    Contended,
    Poisoned,
}
