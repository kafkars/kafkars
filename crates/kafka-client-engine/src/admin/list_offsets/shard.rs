//! Linear synchronized ownership of one Admin `ListOffsets` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminListOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminListOffsetsAdmissionErrorKind, AdminListOffsetsHost, AdminListOffsetsHostError,
    host::AdminListOffsetsAdmission,
};

pub(crate) trait AdminListOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminListOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminListOffsetsShardWakeError {
    source: io::Error,
}

impl AdminListOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminListOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminListOffsetsShardWakeError {}

struct AdminListOffsetsShardState {
    host: Mutex<AdminListOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminListOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminListOffsetsAdmissionPort {
    shared: Arc<AdminListOffsetsShardState>,
}

impl AdminListOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminListOffsetsPlan,
    ) -> Result<AdminListOffsetsAdmission, AdminListOffsetsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminListOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminListOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminListOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminListOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminListOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminListOffsetsShardOwner {
    shared: Arc<AdminListOffsetsShardState>,
}

impl AdminListOffsetsShardOwner {
    pub(crate) fn new<W>(host: AdminListOffsetsHost, wake: Arc<W>) -> Self
    where
        W: AdminListOffsetsShardWake,
    {
        Self {
            shared: Arc::new(AdminListOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminListOffsetsAdmissionPort {
        AdminListOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AdminListOffsetsHost>, AdminListOffsetsShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AdminListOffsetsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AdminListOffsetsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminListOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminListOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminListOffsetsShardState {
    fn host(&self) -> Result<MutexGuard<'_, AdminListOffsetsHost>, AdminListOffsetsShardLockError> {
        self.host
            .lock()
            .map_err(|_poisoned| AdminListOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListOffsetsShardLockError {
    Contended,
    Poisoned,
}
