//! Stable exact-vector rejection for homogeneous transactional batch admission.

use core::fmt;

use crate::producer::PublicProducerRecord;

use super::TransactionSendAdmissionErrorKind;

/// Rejected homogeneous transactional batch retaining every original record.
#[must_use = "transactional batch rejection retains the exact original record vector"]
pub struct TransactionBatchSendAdmissionError {
    kind: TransactionSendAdmissionErrorKind,
    records: Vec<PublicProducerRecord>,
}

impl TransactionBatchSendAdmissionError {
    pub(super) const fn new(
        kind: TransactionSendAdmissionErrorKind,
        records: Vec<PublicProducerRecord>,
    ) -> Self {
        Self { kind, records }
    }

    /// Returns the stable admission rejection category.
    pub const fn kind(&self) -> TransactionSendAdmissionErrorKind {
        self.kind
    }

    /// Borrows every exact original record in caller order.
    pub fn records(&self) -> &[PublicProducerRecord] {
        &self.records
    }

    /// Recovers the exact original vector for retry or rerouting.
    pub fn into_records(self) -> Vec<PublicProducerRecord> {
        self.records
    }

    /// Recovers both the stable category and exact original vector.
    pub fn into_parts(self) -> (TransactionSendAdmissionErrorKind, Vec<PublicProducerRecord>) {
        (self.kind, self.records)
    }
}

impl fmt::Debug for TransactionBatchSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionBatchSendAdmissionError")
            .field("kind", &self.kind)
            .field("record_count", &self.records.len())
            .finish()
    }
}

impl fmt::Display for TransactionBatchSendAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "transactional batch send rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for TransactionBatchSendAdmissionError {}
