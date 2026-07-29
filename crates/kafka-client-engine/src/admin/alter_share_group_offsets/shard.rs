//! Linear synchronized ownership of one share-group offset alteration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterShareGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AlterShareGroupOffsetsAdmissionErrorKind, AlterShareGroupOffsetsHost,
    AlterShareGroupOffsetsHostError, host::AlterShareGroupOffsetsAdmission,
};

pub(crate) trait AlterShareGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterShareGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterShareGroupOffsetsShardWakeError {
    source: io::Error,
}

impl AlterShareGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterShareGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AlterShareGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AlterShareGroupOffsetsShardWakeError {}

struct AlterShareGroupOffsetsShardState {
    host: Mutex<AlterShareGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterShareGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterShareGroupOffsetsAdmissionPort {
    shared: Arc<AlterShareGroupOffsetsShardState>,
}

impl AlterShareGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterShareGroupOffsetsPlan,
    ) -> Result<AlterShareGroupOffsetsAdmission, AlterShareGroupOffsetsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterShareGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterShareGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterShareGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterShareGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterShareGroupOffsetsShardOwner {
    shared: Arc<AlterShareGroupOffsetsShardState>,
}

impl AlterShareGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: AlterShareGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: AlterShareGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(AlterShareGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterShareGroupOffsetsAdmissionPort {
        AlterShareGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AlterShareGroupOffsetsHost>, AlterShareGroupOffsetsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AlterShareGroupOffsetsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AlterShareGroupOffsetsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterShareGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterShareGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterShareGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AlterShareGroupOffsetsHost>, AlterShareGroupOffsetsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_| AlterShareGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterShareGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
