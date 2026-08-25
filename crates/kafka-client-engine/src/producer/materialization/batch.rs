//! Idempotent materialization construction and exact ownership transfer.

use std::sync::Arc;

use kafka_client_core::{
    ProducerIdentity, ProducerSequenceLease, partitioning::TopicMetadataGeneration,
};

use super::{MaterializationBatch, MaterializationRecord};

impl MaterializationBatch {
    #[cfg(test)]
    pub(crate) fn try_for_test(
        topic: impl Into<Arc<str>>,
        partition: i32,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
    ) -> Option<Self> {
        let identity = ProducerIdentity::try_new(1, 0)?;
        let count = u32::try_from(records.len().max(1)).ok()?;
        let sequence = ProducerSequenceLease::try_new(0, count)?;
        Some(Self::idempotent(
            topic,
            partition,
            None,
            records,
            max_batch_bytes,
            max_batch_bytes,
            identity,
            sequence,
        ))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "construction names the exact record bytes, byte limits, identity, and sequence authorities"
    )]
    pub(crate) fn idempotent(
        topic: impl Into<Arc<str>>,
        partition: i32,
        leader_broker_id: Option<i32>,
        records: Vec<MaterializationRecord>,
        max_batch_bytes: usize,
        source_retained_bytes: usize,
        identity: ProducerIdentity,
        sequence: ProducerSequenceLease,
    ) -> Self {
        Self {
            topic: topic.into(),
            expected_topic_uuid: None,
            validated_topic_generation: None,
            partition,
            leader_broker_id,
            records,
            max_batch_bytes,
            source_retained_bytes,
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

    /// Consumes the batch into the existing protocol materializer's fields.
    pub(crate) fn into_idempotent_parts(
        self,
    ) -> (
        Arc<str>,
        i32,
        Option<i32>,
        Vec<MaterializationRecord>,
        usize,
        ProducerIdentity,
        ProducerSequenceLease,
    ) {
        (
            self.topic,
            self.partition,
            self.leader_broker_id,
            self.records,
            self.max_batch_bytes,
            self.identity,
            self.sequence,
        )
    }

    /// Returns the canonical source bytes fenced while a worker owns shared views.
    pub(crate) const fn source_retained_bytes(&self) -> usize {
        self.source_retained_bytes
    }

    /// Returns the maximum encoded output retained by this exact job.
    pub(crate) const fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (Arc<str>, i32, Vec<MaterializationRecord>, usize) {
        (
            self.topic,
            self.partition,
            self.records,
            self.max_batch_bytes,
        )
    }
}
