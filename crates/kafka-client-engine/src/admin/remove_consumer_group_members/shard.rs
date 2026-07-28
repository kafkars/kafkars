//! Synchronized linear ownership of one static-member removal host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{Moment, RemoveConsumerGroupMembersPlan};

use crate::clock::OperationDeadline;

use super::{
    RemoveConsumerGroupMembersAdmissionErrorKind, RemoveConsumerGroupMembersHost,
    RemoveConsumerGroupMembersHostError, host::RemoveConsumerGroupMembersAdmission,
};

pub(crate) trait RemoveConsumerGroupMembersShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), RemoveConsumerGroupMembersShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct RemoveConsumerGroupMembersShardWakeError {
    source: io::Error,
}

impl RemoveConsumerGroupMembersShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for RemoveConsumerGroupMembersShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "member-removal shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for RemoveConsumerGroupMembersShardWakeError {}

struct RemoveConsumerGroupMembersShardState {
    host: Mutex<RemoveConsumerGroupMembersHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn RemoveConsumerGroupMembersShardWake>,
}

#[derive(Clone)]
pub(crate) struct RemoveConsumerGroupMembersAdmissionPort {
    shared: Arc<RemoveConsumerGroupMembersShardState>,
}

impl RemoveConsumerGroupMembersAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: RemoveConsumerGroupMembersPlan,
    ) -> Result<RemoveConsumerGroupMembersAdmission, RemoveConsumerGroupMembersAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(RemoveConsumerGroupMembersAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(RemoveConsumerGroupMembersAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(RemoveConsumerGroupMembersAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(RemoveConsumerGroupMembersHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), RemoveConsumerGroupMembersShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct RemoveConsumerGroupMembersShardOwner {
    shared: Arc<RemoveConsumerGroupMembersShardState>,
}

impl RemoveConsumerGroupMembersShardOwner {
    pub(crate) fn new<W>(host: RemoveConsumerGroupMembersHost, wake: Arc<W>) -> Self
    where
        W: RemoveConsumerGroupMembersShardWake,
    {
        Self {
            shared: Arc::new(RemoveConsumerGroupMembersShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> RemoveConsumerGroupMembersAdmissionPort {
        RemoveConsumerGroupMembersAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, RemoveConsumerGroupMembersHost>,
        RemoveConsumerGroupMembersShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(RemoveConsumerGroupMembersShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(RemoveConsumerGroupMembersShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut RemoveConsumerGroupMembersHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, RemoveConsumerGroupMembersHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl RemoveConsumerGroupMembersShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, RemoveConsumerGroupMembersHost>,
        RemoveConsumerGroupMembersShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| RemoveConsumerGroupMembersShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveConsumerGroupMembersShardLockError {
    Contended,
    Poisoned,
}
