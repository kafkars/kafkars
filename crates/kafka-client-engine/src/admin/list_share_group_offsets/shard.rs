//! Linear synchronized ownership of one share-group offset listing host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ListShareGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    ListShareGroupOffsetsAdmissionErrorKind, ListShareGroupOffsetsHost,
    ListShareGroupOffsetsHostError, host::ListShareGroupOffsetsAdmission,
};

pub(crate) trait ListShareGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListShareGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListShareGroupOffsetsShardWakeError {
    source: io::Error,
}

impl ListShareGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListShareGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListShareGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListShareGroupOffsetsShardWakeError {}

struct ListShareGroupOffsetsShardState {
    host: Mutex<ListShareGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListShareGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListShareGroupOffsetsAdmissionPort {
    shared: Arc<ListShareGroupOffsetsShardState>,
}

impl ListShareGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListShareGroupOffsetsPlan,
    ) -> Result<ListShareGroupOffsetsAdmission, ListShareGroupOffsetsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListShareGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListShareGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListShareGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListShareGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListShareGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListShareGroupOffsetsShardOwner {
    shared: Arc<ListShareGroupOffsetsShardState>,
}

impl ListShareGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: ListShareGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: ListShareGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(ListShareGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListShareGroupOffsetsAdmissionPort {
        ListShareGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ListShareGroupOffsetsHost>, ListShareGroupOffsetsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ListShareGroupOffsetsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ListShareGroupOffsetsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListShareGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListShareGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListShareGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, ListShareGroupOffsetsHost>, ListShareGroupOffsetsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_| ListShareGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListShareGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
