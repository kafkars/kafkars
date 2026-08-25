//! Transactional materialization construction and exact ownership transfer.

use std::sync::Arc;

use kafka_client_core::{TransactionSequenceLease, TransactionalProducerIdentity};

use super::{MaterializationRecord, TransactionalMaterializationBatch};

impl TransactionalMaterializationBatch {
    pub(crate) fn new(
        topic: impl Into<Arc<str>>,
        partition: i32,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
        identity: TransactionalProducerIdentity,
        sequence: TransactionSequenceLease,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        }
    }

    /// Borrows the exact name used by partition enrollment and Produce routing.
    pub(crate) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    /// Returns the exact partition used by enrollment, sequencing, and Produce.
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<str>,
        i32,
        Vec<MaterializationRecord>,
        usize,
        TransactionalProducerIdentity,
        TransactionSequenceLease,
    ) {
        (
            self.topic,
            self.partition,
            self.records,
            self.max_batch_bytes,
            self.identity,
            self.sequence,
        )
    }

    pub(crate) const fn identity(&self) -> TransactionalProducerIdentity {
        self.identity
    }
}
