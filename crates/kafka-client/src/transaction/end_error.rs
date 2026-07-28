//! End-admission rejection retaining the exact active transaction.

use core::fmt;

use crate::KafkaError;

use super::Transaction;

/// Rejected commit or abort that returns the active transaction unchanged.
#[must_use = "recover the transaction for retry or abort"]
pub struct TransactionEndAdmissionError<'producer> {
    transaction: Transaction<'producer>,
    error: KafkaError,
}

impl<'producer> TransactionEndAdmissionError<'producer> {
    pub(crate) const fn new(transaction: Transaction<'producer>, error: KafkaError) -> Self {
        Self { transaction, error }
    }

    /// Returns the stable semantic admission error.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Recovers the exact active transaction for retry or abort.
    pub fn into_transaction(self) -> Transaction<'producer> {
        self.transaction
    }

    /// Recovers both the exact transaction and semantic admission error.
    pub fn into_parts(self) -> (Transaction<'producer>, KafkaError) {
        (self.transaction, self.error)
    }
}

impl fmt::Debug for TransactionEndAdmissionError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionEndAdmissionError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TransactionEndAdmissionError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for TransactionEndAdmissionError<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
