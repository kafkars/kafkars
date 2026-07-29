//! Linear synchronized ownership of one metadata-quorum voter-removal host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{Moment, RemoveRaftVoterPlan};

use crate::clock::OperationDeadline;

use super::{
    RemoveRaftVoterAdmissionErrorKind, RemoveRaftVoterHost, RemoveRaftVoterHostError,
    host::RemoveRaftVoterAdmission,
};

pub(crate) trait RemoveRaftVoterShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), RemoveRaftVoterShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct RemoveRaftVoterShardWakeError {
    source: io::Error,
}

impl RemoveRaftVoterShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for RemoveRaftVoterShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin RemoveRaftVoter shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for RemoveRaftVoterShardWakeError {}

struct RemoveRaftVoterShardState {
    host: Mutex<RemoveRaftVoterHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn RemoveRaftVoterShardWake>,
}

#[derive(Clone)]
pub(crate) struct RemoveRaftVoterAdmissionPort {
    shared: Arc<RemoveRaftVoterShardState>,
}

impl RemoveRaftVoterAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RemoveRaftVoterPlan,
    ) -> Result<RemoveRaftVoterAdmission, RemoveRaftVoterAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(RemoveRaftVoterAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(RemoveRaftVoterAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(RemoveRaftVoterAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(RemoveRaftVoterHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), RemoveRaftVoterShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct RemoveRaftVoterShardOwner {
    shared: Arc<RemoveRaftVoterShardState>,
}

impl RemoveRaftVoterShardOwner {
    pub(crate) fn new<W>(host: RemoveRaftVoterHost, wake: Arc<W>) -> Self
    where
        W: RemoveRaftVoterShardWake,
    {
        Self {
            shared: Arc::new(RemoveRaftVoterShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> RemoveRaftVoterAdmissionPort {
        RemoveRaftVoterAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, RemoveRaftVoterHost>, RemoveRaftVoterShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(RemoveRaftVoterShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(RemoveRaftVoterShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut RemoveRaftVoterHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, RemoveRaftVoterHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl RemoveRaftVoterShardState {
    fn host(&self) -> Result<MutexGuard<'_, RemoveRaftVoterHost>, RemoveRaftVoterShardLockError> {
        self.host
            .lock()
            .map_err(|_| RemoveRaftVoterShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveRaftVoterShardLockError {
    Contended,
    Poisoned,
}
