//! Transactional materialization construction and exact ownership transfer.

use std::sync::Arc;

use kafka_client_core::{
    TransactionSequenceLease, TransactionalProducerIdentity, partitioning::TopicMetadataGeneration,
};

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
            expected_topic_uuid: None,
            validated_topic_generation: None,
            partition,
            records,
            max_batch_bytes,
            identity,
            sequence,
        }
    }

    pub(crate) const fn with_expected_topic_identity(
        mut self,
        expected_topic_uuid: Option<[u8; 16]>,
        validated_topic_generation: Option<TopicMetadataGeneration>,
    ) -> Self {
        self.expected_topic_uuid = expected_topic_uuid;
        self.validated_topic_generation = validated_topic_generation;
        self
    }

    pub(crate) const fn expected_topic_uuid(&self) -> Option<[u8; 16]> {
        self.expected_topic_uuid
    }

    pub(crate) const fn validated_topic_generation(&self) -> Option<TopicMetadataGeneration> {
        self.validated_topic_generation
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
