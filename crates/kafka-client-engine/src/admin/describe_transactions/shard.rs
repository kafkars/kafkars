//! Linear synchronized ownership of one Admin `DescribeTransactions` host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminDescribeTransactionsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminDescribeTransactionsAdmissionErrorKind, AdminDescribeTransactionsHost,
    AdminDescribeTransactionsHostError, host::AdminDescribeTransactionsAdmission,
};

pub(crate) trait AdminDescribeTransactionsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminDescribeTransactionsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminDescribeTransactionsShardWakeError {
    source: io::Error,
}

impl AdminDescribeTransactionsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminDescribeTransactionsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin DescribeTransactions shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminDescribeTransactionsShardWakeError {}

struct AdminDescribeTransactionsShardState {
    host: Mutex<AdminDescribeTransactionsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminDescribeTransactionsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminDescribeTransactionsAdmissionPort {
    shared: Arc<AdminDescribeTransactionsShardState>,
}

impl AdminDescribeTransactionsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeTransactionsPlan,
    ) -> Result<AdminDescribeTransactionsAdmission, AdminDescribeTransactionsAdmissionErrorKind>
    {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminDescribeTransactionsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminDescribeTransactionsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminDescribeTransactionsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminDescribeTransactionsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminDescribeTransactionsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminDescribeTransactionsShardOwner {
    shared: Arc<AdminDescribeTransactionsShardState>,
}

impl AdminDescribeTransactionsShardOwner {
    pub(crate) fn new<W>(host: AdminDescribeTransactionsHost, wake: Arc<W>) -> Self
    where
        W: AdminDescribeTransactionsShardWake,
    {
        Self {
            shared: Arc::new(AdminDescribeTransactionsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminDescribeTransactionsAdmissionPort {
        AdminDescribeTransactionsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<
        MutexGuard<'_, AdminDescribeTransactionsHost>,
        AdminDescribeTransactionsShardLockError,
    > {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => {
                Err(AdminDescribeTransactionsShardLockError::Contended)
            }
            Err(TryLockError::Poisoned(_)) => {
                Err(AdminDescribeTransactionsShardLockError::Poisoned)
            }
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminDescribeTransactionsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminDescribeTransactionsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminDescribeTransactionsShardState {
    fn host(
        &self,
    ) -> Result<
        MutexGuard<'_, AdminDescribeTransactionsHost>,
        AdminDescribeTransactionsShardLockError,
    > {
        self.host
            .lock()
            .map_err(|_poisoned| AdminDescribeTransactionsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminDescribeTransactionsShardLockError {
    Contended,
    Poisoned,
}
