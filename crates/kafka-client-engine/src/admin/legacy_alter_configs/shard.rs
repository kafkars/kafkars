//! Linear synchronized ownership of one legacy full-snapshot configuration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{LegacyAlterConfigsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    LegacyAlterConfigsAdmissionErrorKind, LegacyAlterConfigsHost, LegacyAlterConfigsHostError,
    host::LegacyAlterConfigsAdmission, model::LegacyAlterConfigsRetention,
};

pub(crate) trait LegacyAlterConfigsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), LegacyAlterConfigsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct LegacyAlterConfigsShardWakeError {
    source: io::Error,
}

impl LegacyAlterConfigsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for LegacyAlterConfigsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LegacyAlterConfigs shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for LegacyAlterConfigsShardWakeError {}

struct LegacyAlterConfigsShardState {
    host: Mutex<LegacyAlterConfigsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn LegacyAlterConfigsShardWake>,
}

#[derive(Clone)]
pub(crate) struct LegacyAlterConfigsAdmissionPort {
    shared: Arc<LegacyAlterConfigsShardState>,
}

impl LegacyAlterConfigsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: LegacyAlterConfigsPlan,
        retention: LegacyAlterConfigsRetention,
    ) -> Result<LegacyAlterConfigsAdmission, LegacyAlterConfigsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(LegacyAlterConfigsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(LegacyAlterConfigsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(LegacyAlterConfigsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan, retention)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(LegacyAlterConfigsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), LegacyAlterConfigsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct LegacyAlterConfigsShardOwner {
    shared: Arc<LegacyAlterConfigsShardState>,
}

impl LegacyAlterConfigsShardOwner {
    pub(crate) fn new<W>(host: LegacyAlterConfigsHost, wake: Arc<W>) -> Self
    where
        W: LegacyAlterConfigsShardWake,
    {
        Self {
            shared: Arc::new(LegacyAlterConfigsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> LegacyAlterConfigsAdmissionPort {
        LegacyAlterConfigsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, LegacyAlterConfigsHost>, LegacyAlterConfigsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(LegacyAlterConfigsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(LegacyAlterConfigsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut LegacyAlterConfigsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, LegacyAlterConfigsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl LegacyAlterConfigsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, LegacyAlterConfigsHost>, LegacyAlterConfigsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| LegacyAlterConfigsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyAlterConfigsShardLockError {
    Contended,
    Poisoned,
}
