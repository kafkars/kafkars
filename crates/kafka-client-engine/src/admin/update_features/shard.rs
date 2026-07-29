//! Linear synchronized ownership of one finalized-feature mutation host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{Moment, UpdateFeaturesPlan};

use crate::clock::OperationDeadline;

use super::{
    UpdateFeaturesAdmissionErrorKind, UpdateFeaturesHost, UpdateFeaturesHostError,
    host::UpdateFeaturesAdmission,
};

pub(crate) trait UpdateFeaturesShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), UpdateFeaturesShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct UpdateFeaturesShardWakeError {
    source: io::Error,
}

impl UpdateFeaturesShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for UpdateFeaturesShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin UpdateFeatures shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for UpdateFeaturesShardWakeError {}

struct UpdateFeaturesShardState {
    host: Mutex<UpdateFeaturesHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn UpdateFeaturesShardWake>,
}

#[derive(Clone)]
pub(crate) struct UpdateFeaturesAdmissionPort {
    shared: Arc<UpdateFeaturesShardState>,
}

impl UpdateFeaturesAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: UpdateFeaturesPlan,
    ) -> Result<UpdateFeaturesAdmission, UpdateFeaturesAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(UpdateFeaturesAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(UpdateFeaturesAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(UpdateFeaturesAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(UpdateFeaturesHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), UpdateFeaturesShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct UpdateFeaturesShardOwner {
    shared: Arc<UpdateFeaturesShardState>,
}

impl UpdateFeaturesShardOwner {
    pub(crate) fn new<W>(host: UpdateFeaturesHost, wake: Arc<W>) -> Self
    where
        W: UpdateFeaturesShardWake,
    {
        Self {
            shared: Arc::new(UpdateFeaturesShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> UpdateFeaturesAdmissionPort {
        UpdateFeaturesAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, UpdateFeaturesHost>, UpdateFeaturesShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(UpdateFeaturesShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(UpdateFeaturesShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut UpdateFeaturesHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, UpdateFeaturesHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl UpdateFeaturesShardState {
    fn host(&self) -> Result<MutexGuard<'_, UpdateFeaturesHost>, UpdateFeaturesShardLockError> {
        self.host
            .lock()
            .map_err(|_| UpdateFeaturesShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesShardLockError {
    Contended,
    Poisoned,
}
