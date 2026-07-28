//! Linear active transaction borrowing one initialized producer.

use std::time::Duration;

use crate::bridge::transaction::TransactionEngine;

use super::{AbortTransaction, CommitTransaction, TransactionEndAdmissionError};

/// Opaque active transaction that exclusively borrows its producer.
///
/// Dropping an active transaction does not report success. The engine treats
/// token loss as owner loss and schedules a bounded best-effort abort.
#[derive(Debug)]
#[must_use = "commit, abort, or drop the active transaction"]
pub struct Transaction<'producer> {
    inner: TransactionEngine<'producer>,
}

impl<'producer> Transaction<'producer> {
    pub(crate) const fn from_bridge(inner: TransactionEngine<'producer>) -> Self {
        Self { inner }
    }

    /// Reports an advisory reactor-wake failure after accepted begin.
    pub const fn begin_wake_failed(&self) -> bool {
        self.inner.begin_wake_failed()
    }

    /// Attempts to commit this exact active transaction.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or abort.
    pub fn commit(
        self,
        timeout: Duration,
    ) -> Result<CommitTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        self.inner
            .commit(timeout)
            .map(CommitTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }

    /// Attempts to abort this exact active transaction.
    ///
    /// Rejection returns [`TransactionEndAdmissionError`] containing this same
    /// transaction for retry or another abort attempt.
    pub fn abort(
        self,
        timeout: Duration,
    ) -> Result<AbortTransaction<'producer>, TransactionEndAdmissionError<'producer>> {
        self.inner
            .abort(timeout)
            .map(AbortTransaction::from_bridge)
            .map_err(|(transaction, error)| {
                TransactionEndAdmissionError::new(Self::from_bridge(transaction), error)
            })
    }
}
