//! Transactional-send admission rejection with exact record recovery.

use core::fmt;

use crate::{KafkaError, Record};

/// Rejected transactional send retaining the caller's record.
#[must_use = "recover the record before handling the admission failure"]
pub struct TransactionSendAdmissionError {
    record: Record,
    error: KafkaError,
}

impl TransactionSendAdmissionError {
    pub(crate) const fn new(record: Record, error: KafkaError) -> Self {
        Self { record, error }
    }

    /// Returns the stable semantic admission error.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Recovers the exact rejected record.
    pub fn into_record(self) -> Record {
        self.record
    }

    /// Recovers both the exact record and semantic admission error.
    pub fn into_parts(self) -> (Record, KafkaError) {
        (self.record, self.error)
    }
}

impl fmt::Debug for TransactionSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionSendAdmissionError")
            .field("record", &self.record)
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for TransactionSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for TransactionSendAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
