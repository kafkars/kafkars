//! Linear synchronized ownership of one SCRAM credential-alteration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterUserScramCredentialsPlan, Moment};

use crate::{
    clock::OperationDeadline,
    protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest,
};

use super::{
    AlterUserScramCredentialsAdmissionErrorKind, AlterUserScramCredentialsHost,
    AlterUserScramCredentialsHostError, host::AlterUserScramCredentialsAdmission,
};

pub(crate) trait AlterUserScramCredentialsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterUserScramCredentialsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterUserScramCredentialsShardWakeError {
    source: io::Error,
}

impl AlterUserScramCredentialsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterUserScramCredentialsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterUserScramCredentials shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AlterUserScramCredentialsShardWakeError {}

struct AlterUserScramCredentialsShardState {
    host: Mutex<AlterUserScramCredentialsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterUserScramCredentialsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterUserScramCredentialsAdmissionPort {
    shared: Arc<AlterUserScramCredentialsShardState>,
}

impl AlterUserScramCredentialsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterUserScramCredentialsPlan,
        prepared_request: PreparedAlterUserScramCredentialsRequest,
    ) -> Result<AlterUserScramCredentialsAdmission, AlterUserScramCredentialsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterUserScramCredentialsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterUserScramCredentialsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterUserScramCredentialsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, prepared_request)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterUserScramCredentialsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterUserScramCredentialsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterUserScramCredentialsShardOwner {
    shared: Arc<AlterUserScramCredentialsShardState>,
}

impl AlterUserScramCredentialsShardOwner {
    pub(crate) fn new<W>(host: AlterUserScramCredentialsHost, wake: Arc<W>) -> Self
    where
        W: AlterUserScramCredentialsShardWake,
    {
        Self {
            shared: Arc::new(AlterUserScramCredentialsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterUserScramCredentialsAdmissionPort {
        AlterUserScramCredentialsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterUserScramCredentialsHost>,
        AlterUserScramCredentialsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AlterUserScramCredentialsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AlterUserScramCredentialsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterUserScramCredentialsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterUserScramCredentialsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterUserScramCredentialsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterUserScramCredentialsHost>,
        AlterUserScramCredentialsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| AlterUserScramCredentialsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialsShardLockError {
    Contended,
    Poisoned,
}
