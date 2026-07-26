//! Linear synchronized ownership of one offset-alteration host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AlterConsumerGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AlterConsumerGroupOffsetsAdmissionErrorKind, AlterConsumerGroupOffsetsHost,
    AlterConsumerGroupOffsetsHostError, host::AlterConsumerGroupOffsetsAdmission,
};

pub(crate) trait AlterConsumerGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AlterConsumerGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AlterConsumerGroupOffsetsShardWakeError {
    source: io::Error,
}

impl AlterConsumerGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AlterConsumerGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AlterConsumerGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AlterConsumerGroupOffsetsShardWakeError {}

struct AlterConsumerGroupOffsetsShardState {
    host: Mutex<AlterConsumerGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AlterConsumerGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AlterConsumerGroupOffsetsAdmissionPort {
    shared: Arc<AlterConsumerGroupOffsetsShardState>,
}

impl AlterConsumerGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AlterConsumerGroupOffsetsPlan,
    ) -> Result<AlterConsumerGroupOffsetsAdmission, AlterConsumerGroupOffsetsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AlterConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AlterConsumerGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AlterConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AlterConsumerGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AlterConsumerGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AlterConsumerGroupOffsetsShardOwner {
    shared: Arc<AlterConsumerGroupOffsetsShardState>,
}

impl AlterConsumerGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: AlterConsumerGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: AlterConsumerGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(AlterConsumerGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AlterConsumerGroupOffsetsAdmissionPort {
        AlterConsumerGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterConsumerGroupOffsetsHost>,
        AlterConsumerGroupOffsetsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AlterConsumerGroupOffsetsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AlterConsumerGroupOffsetsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AlterConsumerGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AlterConsumerGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AlterConsumerGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AlterConsumerGroupOffsetsHost>,
        AlterConsumerGroupOffsetsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| AlterConsumerGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterConsumerGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
