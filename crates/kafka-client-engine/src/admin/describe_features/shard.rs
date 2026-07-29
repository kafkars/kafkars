//! Linear synchronized ownership of one feature host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::Moment;

use crate::clock::OperationDeadline;

use super::{
    DescribeFeaturesAdmissionErrorKind, DescribeFeaturesHost, DescribeFeaturesHostError,
    host::DescribeFeaturesAdmission,
};

pub(crate) trait DescribeFeaturesShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeFeaturesShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeFeaturesShardWakeError {
    source: io::Error,
}

impl DescribeFeaturesShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeFeaturesShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeFeatures shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeFeaturesShardWakeError {}

struct DescribeFeaturesShardState {
    host: Mutex<DescribeFeaturesHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeFeaturesShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeFeaturesAdmissionPort {
    shared: Arc<DescribeFeaturesShardState>,
}

impl DescribeFeaturesAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeFeaturesAdmission, DescribeFeaturesAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeFeaturesAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeFeaturesAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeFeaturesAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeFeaturesHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeFeaturesShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeFeaturesShardOwner {
    shared: Arc<DescribeFeaturesShardState>,
}

impl DescribeFeaturesShardOwner {
    pub(crate) fn new<W>(host: DescribeFeaturesHost, wake: Arc<W>) -> Self
    where
        W: DescribeFeaturesShardWake,
    {
        Self {
            shared: Arc::new(DescribeFeaturesShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeFeaturesAdmissionPort {
        DescribeFeaturesAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeFeaturesHost>, DescribeFeaturesShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeFeaturesShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeFeaturesShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeFeaturesHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeFeaturesHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeFeaturesShardState {
    fn host(&self) -> Result<MutexGuard<'_, DescribeFeaturesHost>, DescribeFeaturesShardLockError> {
        self.host
            .lock()
            .map_err(|_| DescribeFeaturesShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeFeaturesShardLockError {
    Contended,
    Poisoned,
}
