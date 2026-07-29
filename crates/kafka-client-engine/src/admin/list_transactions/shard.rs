//! Linear synchronized ownership of one cluster transaction-listing host.

use std::{
    fmt, io,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_client_core::{AdminListTransactionsPlan, Moment};

use crate::clock::OperationDeadline;

use super::{
    AdminListTransactionsAdmissionErrorKind, AdminListTransactionsHost,
    AdminListTransactionsHostError, host::AdminListTransactionsAdmission,
};

pub(crate) trait AdminListTransactionsShardWake: Send + Sync + 'static {
    fn wake(&self) -> Result<(), AdminListTransactionsShardWakeError>;
}

#[derive(Debug)]
pub(crate) struct AdminListTransactionsShardWakeError {
    source: io::Error,
}

impl AdminListTransactionsShardWakeError {
    pub(crate) const fn from_io(source: io::Error) -> Self {
        Self { source }
    }
}

impl fmt::Display for AdminListTransactionsShardWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Admin ListTransactions shard wake failed: {}",
            self.source
        )
    }
}

impl std::error::Error for AdminListTransactionsShardWakeError {}

struct AdminListTransactionsShardState {
    host: Mutex<AdminListTransactionsHost>,
    admission_closed: AtomicBool,
    wake: Arc<dyn AdminListTransactionsShardWake>,
}

#[derive(Clone)]
pub(crate) struct AdminListTransactionsAdmissionPort {
    shared: Arc<AdminListTransactionsShardState>,
}

impl AdminListTransactionsAdmissionPort {
    pub(crate) fn try_admit(
        &self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminListTransactionsPlan,
    ) -> Result<AdminListTransactionsAdmission, AdminListTransactionsAdmissionErrorKind> {
        if self.shared.admission_closed.load(Ordering::Acquire) {
            return Err(AdminListTransactionsAdmissionErrorKind::Closed);
        }
        let mut host = match self.shared.host.try_lock() {
            Ok(host) => host,
            Err(TryLockError::WouldBlock) => {
                return Err(AdminListTransactionsAdmissionErrorKind::Contended);
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(AdminListTransactionsAdmissionErrorKind::HostUnavailable);
            }
        };
        let mut admission = host.try_admit(now, deadline, plan)?;
        drop(host);
        if self.shared.wake.wake().is_err() {
            admission
                .fault
                .get_or_insert(AdminListTransactionsHostError::Wake);
        }
        Ok(admission)
    }

    pub(crate) fn close_admission(&self) -> Result<(), AdminListTransactionsShardLockError> {
        let mut host = self.shared.host()?;
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
        Ok(())
    }
}

pub(crate) struct AdminListTransactionsShardOwner {
    shared: Arc<AdminListTransactionsShardState>,
}

impl AdminListTransactionsShardOwner {
    pub(crate) fn new<W>(host: AdminListTransactionsHost, wake: Arc<W>) -> Self
    where
        W: AdminListTransactionsShardWake,
    {
        Self {
            shared: Arc::new(AdminListTransactionsShardState {
                host: Mutex::new(host),
                admission_closed: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub(crate) fn admission_port(&self) -> AdminListTransactionsAdmissionPort {
        AdminListTransactionsAdmissionPort {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn try_host(
        &self,
    ) -> Result<MutexGuard<'_, AdminListTransactionsHost>, AdminListTransactionsShardLockError>
    {
        match self.shared.host.try_lock() {
            Ok(host) => Ok(host),
            Err(TryLockError::WouldBlock) => Err(AdminListTransactionsShardLockError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(AdminListTransactionsShardLockError::Poisoned),
        }
    }

    pub(crate) fn close_locked(&self, host: &mut AdminListTransactionsHost) {
        host.close_admission();
        self.shared.admission_closed.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_host(&self) -> MutexGuard<'_, AdminListTransactionsHost> {
        let mut host = self
            .shared
            .host
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.close_locked(&mut host);
        host
    }
}

impl AdminListTransactionsShardState {
    fn host(
        &self,
    ) -> Result<MutexGuard<'_, AdminListTransactionsHost>, AdminListTransactionsShardLockError>
    {
        self.host
            .lock()
            .map_err(|_| AdminListTransactionsShardLockError::Poisoned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListTransactionsShardLockError {
    Contended,
    Poisoned,
}
