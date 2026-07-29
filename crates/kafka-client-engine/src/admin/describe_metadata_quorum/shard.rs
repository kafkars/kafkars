//! Linear synchronized ownership of one Admin `DescribeMetadataQuorum` host.

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
    DescribeMetadataQuorumAdmissionErrorKind, DescribeMetadataQuorumHost,
    DescribeMetadataQuorumHostError, host::DescribeMetadataQuorumAdmission,
};

pub(crate) trait DescribeMetadataQuorumShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), DescribeMetadataQuorumShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct DescribeMetadataQuorumShardWakeError {
    source: io::Error,
}

impl DescribeMetadataQuorumShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for DescribeMetadataQuorumShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DescribeMetadataQuorum shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for DescribeMetadataQuorumShardWakeError {}

struct DescribeMetadataQuorumShardState {
    host: Mutex<DescribeMetadataQuorumHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn DescribeMetadataQuorumShardWake>,
}

#[derive(Clone)]
pub(crate) struct DescribeMetadataQuorumAdmissionPort {
    shared: Arc<DescribeMetadataQuorumShardState>,
}

impl DescribeMetadataQuorumAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<DescribeMetadataQuorumAdmission, DescribeMetadataQuorumAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(DescribeMetadataQuorumAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(DescribeMetadataQuorumAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(DescribeMetadataQuorumAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(DescribeMetadataQuorumHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), DescribeMetadataQuorumShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct DescribeMetadataQuorumShardOwner {
    shared: Arc<DescribeMetadataQuorumShardState>,
}

impl DescribeMetadataQuorumShardOwner {
    pub(crate) fn new<W>(host: DescribeMetadataQuorumHost, wake: Arc<W>) -> Self
    where
        W: DescribeMetadataQuorumShardWake,
    {
        Self {
            shared: Arc::new(DescribeMetadataQuorumShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> DescribeMetadataQuorumAdmissionPort {
        DescribeMetadataQuorumAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeMetadataQuorumHost>, DescribeMetadataQuorumShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(DescribeMetadataQuorumShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(DescribeMetadataQuorumShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut DescribeMetadataQuorumHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, DescribeMetadataQuorumHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl DescribeMetadataQuorumShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, DescribeMetadataQuorumHost>, DescribeMetadataQuorumShardLockError>
    {
        self.host
            .lock()
            .map_err(|_poisoned| DescribeMetadataQuorumShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeMetadataQuorumShardLockError {
    Contended,
    Poisoned,
}
