//! Linear ownership and synchronization for one producer shard.

use std::{
    error::Error,
    fmt, io,
    sync::{Arc, Mutex, MutexGuard, TryLockError},
};

use super::{super::ProducerHost, data::ProducerShardData};

/// Coalescible request for the producer host to make progress.
///
/// Implementations must not run application code. The shared reactor wake is
/// adapted by this concrete ingress domain rather than by the driver owner.
pub(crate) trait ProducerShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ProducerShardWakeError>;
}

/// Driver-neutral retention of a failed post-commit wake request.
#[derive(Debug)]
pub(crate) struct ProducerShardWakeError {
    source: io::Error,
}

impl ProducerShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }

    pub(crate) fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }
}

impl fmt::Display for ProducerShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "producer shard wake failed: {}", self.source)
    }
}

impl Error for ProducerShardWakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

pub(super) struct ProducerShardState {
    data: Mutex<ProducerShardData>,
    wake: Arc<dyn ProducerShardWake>,
}

impl ProducerShardState {
    fn new<W>(host: ProducerHost, wake: Arc<W>) -> Self
    where
        W: ProducerShardWake,
    {
        Self {
            data: Mutex::new(ProducerShardData::new(host)),
            wake,
        }
    }

    pub(super) fn try_data(
        &self,
    ) -> Result<MutexGuard<'_, ProducerShardData>, ProducerShardLockError> {
        match self.data.try_lock() {
            Ok(data) => Ok(data),
            Err(TryLockError::WouldBlock) => Err(ProducerShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ProducerShardLockError::Poisoned),
        }
    }

    pub(super) fn data(&self) -> Result<MutexGuard<'_, ProducerShardData>, ProducerShardLockError> {
        self.data
            .lock()
            .map_err(|_poisoned| ProducerShardLockError::Poisoned)
    }

    pub(super) fn wake(&self) -> Result<(), ProducerShardWakeError> {
        self.wake.wake()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerShardLockError {
    Contended,
    Poisoned,
}

/// Unique host-side owner of one producer shard.
///
/// Admission ports share only this shard's lock. Client-global state is not
/// serialized through the producer hot path.
pub(crate) struct ProducerShardOwner {
    shared: Arc<ProducerShardState>,
}

impl ProducerShardOwner {
    pub(crate) fn new<W>(host: ProducerHost, wake: Arc<W>) -> Self
    where
        W: ProducerShardWake,
    {
        Self {
            shared: Arc::new(ProducerShardState::new(host, wake)),
        }
    }

    pub(crate) fn admission_port(&self) -> super::ProducerAdmissionPort {
        super::ProducerAdmissionPort::new(Arc::clone(&self.shared))
    }

    /// Attempts to acquire this shard for one bounded host turn.
    pub(crate) fn try_data(
        &self,
    ) -> Result<MutexGuard<'_, ProducerShardData>, ProducerShardLockError> {
        self.shared.try_data()
    }

    /// Closes admission during terminal owner cleanup, waiting out a live caller.
    pub(crate) fn close_admission(&self) -> Result<(), ProducerShardLockError> {
        let mut data = self.shared.data()?;
        data.close_admission();
        Ok(())
    }

    /// Recovers terminal ownership even when a prior host panic poisoned it.
    pub(crate) fn terminal_data(&self) -> MutexGuard<'_, ProducerShardData> {
        self.shared
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
