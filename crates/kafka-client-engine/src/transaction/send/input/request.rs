//! Resolved transactional send preparation, routing, recovery, and handoff.

use std::sync::Arc;

use kafka_client_core::{PartitionIndex, TopicId, TransactionEpoch, TransactionPartition};

use crate::{clock::OperationDeadline, producer::materialization::MaterializationRecord};

use super::{TransactionSendInput, TransactionSendRequest};

impl TransactionSendRequest {
    pub(in crate::transaction) fn try_prepare(
        input: TransactionSendInput,
        topic_id: TopicId,
        max_wire_batch_bytes: usize,
    ) -> Result<Self, TransactionSendInput> {
        let TransactionSendInput {
            epoch,
            original_records,
            canonical_topic: topic,
            partition: source_partition,
            materialization_records: records,
            retained_source_bytes,
            deadline,
        } = input;
        let partition =
            source_partition.map(|partition| TransactionPartition::new(topic_id, partition));
        Ok(Self {
            epoch,
            original_records,
            source_partition,
            topic_id,
            partition,
            topic,
            records,
            retained_source_bytes,
            max_wire_batch_bytes,
            deadline,
        })
    }

    pub(crate) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    pub(crate) const fn partition(&self) -> Option<TransactionPartition> {
        self.partition
    }

    pub(in crate::transaction::send) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub(in crate::transaction::send) fn topic(&self) -> &str {
        &self.topic
    }

    pub(in crate::transaction::send) fn expected_topic_uuid(&self) -> Option<[u8; 16]> {
        self.original_records
            .first()
            .and_then(crate::producer::PublicProducerRecord::expected_topic_uuid_value)
    }

    pub(in crate::transaction::send) fn key_bytes(&self) -> Option<&[u8]> {
        self.records
            .first()
            .and_then(MaterializationRecord::key_bytes)
            .map(bytes::Bytes::as_ref)
    }

    pub(in crate::transaction::send) fn assign_partition(
        &mut self,
        partition: PartitionIndex,
    ) -> bool {
        if self.partition.is_some() || self.source_partition.is_some() {
            return false;
        }
        self.source_partition = Some(partition);
        self.partition = Some(TransactionPartition::new(self.topic_id, partition));
        true
    }

    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }

    pub(in crate::transaction::send) fn record_count(&self) -> usize {
        self.records.len()
    }

    pub(in crate::transaction) fn into_input(self) -> TransactionSendInput {
        debug_assert_eq!(self.original_records.len(), self.records.len());
        TransactionSendInput {
            epoch: self.epoch,
            original_records: self.original_records,
            canonical_topic: self.topic,
            partition: self.source_partition,
            materialization_records: self.records,
            retained_source_bytes: self.retained_source_bytes,
            deadline: self.deadline,
        }
    }

    pub(in crate::transaction::send) fn into_parts(
        self,
    ) -> (
        TransactionEpoch,
        TransactionPartition,
        Arc<str>,
        Vec<MaterializationRecord>,
        usize,
        OperationDeadline,
    ) {
        // Execution has admitted the exact retained-byte charge before this
        // handoff. Only now may upstream source leases in the originals end.
        drop(self.original_records);
        let partition = self
            .partition
            .unwrap_or_else(|| unreachable!("resolved send owns one partition"));
        (
            self.epoch,
            partition,
            self.topic,
            self.records,
            self.max_wire_batch_bytes,
            self.deadline,
        )
    }
}
