//! Linear synchronized ownership of one consumer-group offset host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{ListConsumerGroupOffsetsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    ListConsumerGroupOffsetsAdmissionErrorKind, ListConsumerGroupOffsetsHost,
    ListConsumerGroupOffsetsHostError, host::ListConsumerGroupOffsetsAdmission,
};

pub(crate) trait ListConsumerGroupOffsetsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), ListConsumerGroupOffsetsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct ListConsumerGroupOffsetsShardWakeError {
    source: io::Error,
}

impl ListConsumerGroupOffsetsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for ListConsumerGroupOffsetsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ListConsumerGroupOffsets shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for ListConsumerGroupOffsetsShardWakeError {}

struct ListConsumerGroupOffsetsShardState {
    host: Mutex<ListConsumerGroupOffsetsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn ListConsumerGroupOffsetsShardWake>,
}

#[derive(Clone)]
pub(crate) struct ListConsumerGroupOffsetsAdmissionPort {
    shared: Arc<ListConsumerGroupOffsetsShardState>,
}

impl ListConsumerGroupOffsetsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ListConsumerGroupOffsetsPlan,
    ) -> Result<ListConsumerGroupOffsetsAdmission, ListConsumerGroupOffsetsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(ListConsumerGroupOffsetsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(ListConsumerGroupOffsetsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ListConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(ListConsumerGroupOffsetsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), ListConsumerGroupOffsetsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct ListConsumerGroupOffsetsShardOwner {
    shared: Arc<ListConsumerGroupOffsetsShardState>,
}

impl ListConsumerGroupOffsetsShardOwner {
    pub(crate) fn new<W>(host: ListConsumerGroupOffsetsHost, wake: Arc<W>) -> Self
    where
        W: ListConsumerGroupOffsetsShardWake,
    {
        Self {
            shared: Arc::new(ListConsumerGroupOffsetsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> ListConsumerGroupOffsetsAdmissionPort {
        ListConsumerGroupOffsetsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, ListConsumerGroupOffsetsHost>, ListConsumerGroupOffsetsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(ListConsumerGroupOffsetsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(ListConsumerGroupOffsetsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut ListConsumerGroupOffsetsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, ListConsumerGroupOffsetsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl ListConsumerGroupOffsetsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, ListConsumerGroupOffsetsHost>, ListConsumerGroupOffsetsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| ListConsumerGroupOffsetsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupOffsetsShardLockError {
    Contended,
    Poisoned,
}
