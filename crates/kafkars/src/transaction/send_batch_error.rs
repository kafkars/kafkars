//! Transactional batch rejection with ordered original-record recovery.

use core::fmt;

use crate::{KafkaError, Record};

/// Rejected homogeneous transactional batch retaining every caller record.
#[must_use = "recover every original record before handling the admission failure"]
pub struct TransactionBatchSendAdmissionError {
    records: Vec<Record>,
    error: KafkaError,
}

impl TransactionBatchSendAdmissionError {
    pub(crate) const fn new(records: Vec<Record>, error: KafkaError) -> Self {
        Self { records, error }
    }

    /// Returns the stable semantic admission error.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows every original record in exact caller order.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Recovers every original record in caller order.
    pub fn into_records(self) -> Vec<Record> {
        self.records
    }

    /// Recovers both the caller-ordered records and semantic admission error.
    pub fn into_parts(self) -> (Vec<Record>, KafkaError) {
        (self.records, self.error)
    }
}

impl fmt::Debug for TransactionBatchSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionBatchSendAdmissionError")
            .field("record_count", &self.records.len())
            .field("error", &self.error)
            .finish()
    }
}

impl fmt::Display for TransactionBatchSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for TransactionBatchSendAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
