//! Linear synchronized ownership of one committed voter-addition host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AddRaftVoterPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AddRaftVoterAdmissionErrorKind, AddRaftVoterHost, AddRaftVoterHostError,
    host::AddRaftVoterAdmission,
};

pub(crate) trait AddRaftVoterShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AddRaftVoterShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AddRaftVoterShardWakeError {
    source: io::Error,
}

impl AddRaftVoterShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AddRaftVoterShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin AddRaftVoter shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AddRaftVoterShardWakeError {}

struct AddRaftVoterShardState {
    host: Mutex<AddRaftVoterHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AddRaftVoterShardWake>,
}

#[derive(Clone)]
pub(crate) struct AddRaftVoterAdmissionPort {
    shared: Arc<AddRaftVoterShardState>,
}

impl AddRaftVoterAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AddRaftVoterPlan,
    ) -> Result<AddRaftVoterAdmission, AddRaftVoterAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AddRaftVoterAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AddRaftVoterAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AddRaftVoterAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission.fault.get_or_insert(AddRaftVoterHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AddRaftVoterShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AddRaftVoterShardOwner {
    shared: Arc<AddRaftVoterShardState>,
}

impl AddRaftVoterShardOwner {
    pub(crate) fn new<W>(host: AddRaftVoterHost, wake: Arc<W>) -> Self
    where
        W: AddRaftVoterShardWake,
    {
        Self {
            shared: Arc::new(AddRaftVoterShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AddRaftVoterAdmissionPort {
        AddRaftVoterAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AddRaftVoterHost>, AddRaftVoterShardLockError> {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AddRaftVoterShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AddRaftVoterShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AddRaftVoterHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AddRaftVoterHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AddRaftVoterShardState {
    fn host(&self) -> Result<MutexGuard<'_, AddRaftVoterHost>, AddRaftVoterShardLockError> {
        self.host
            .lock()
            .map_err(|_| AddRaftVoterShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddRaftVoterShardLockError {
    Contended,
    Poisoned,
}
