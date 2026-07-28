//! Linear unresolved ingress and one resolved fixed-send request.

use std::sync::Arc;

use kafka_client_core::{PartitionIndex, TopicId, TransactionEpoch, TransactionPartition};

use crate::{
    clock::OperationDeadline,
    producer::{PublicProducerRecord, materialization::MaterializationRecord},
    transaction::TransactionLifecycleHostError,
};

/// Exact caller input retained until deterministic send acceptance.
#[must_use = "transactional send input must be accepted or returned intact"]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionSendInput {
    epoch: TransactionEpoch,
    original_record: PublicProducerRecord,
    canonical_topic: Arc<str>,
    partition: Option<PartitionIndex>,
    materialization_record: MaterializationRecord,
    retained_source_bytes: usize,
    deadline: OperationDeadline,
}

impl TransactionSendInput {
    pub(crate) const fn new(
        epoch: TransactionEpoch,
        original_record: PublicProducerRecord,
        canonical_topic: Arc<str>,
        partition: Option<PartitionIndex>,
        materialization_record: MaterializationRecord,
        retained_source_bytes: usize,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            epoch,
            original_record,
            canonical_topic,
            partition,
            materialization_record,
            retained_source_bytes,
            deadline,
        }
    }

    pub(crate) fn canonical_topic(&self) -> &Arc<str> {
        &self.canonical_topic
    }

    pub(crate) const fn retained_source_bytes(&self) -> usize {
        self.retained_source_bytes
    }

    pub(crate) fn into_original_record(self) -> PublicProducerRecord {
        self.original_record
    }
}

/// One catalog-resolved request before partition and sequence transfer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionSendRequest {
    epoch: TransactionEpoch,
    original_record: PublicProducerRecord,
    source_partition: Option<PartitionIndex>,
    topic_id: TopicId,
    partition: Option<TransactionPartition>,
    topic: Arc<str>,
    records: Vec<MaterializationRecord>,
    retained_source_bytes: usize,
    max_wire_batch_bytes: usize,
    deadline: OperationDeadline,
}

impl TransactionSendRequest {
    #[expect(
        clippy::result_large_err,
        reason = "preparation failure returns the exact caller-owned send input"
    )]
    pub(in crate::transaction) fn try_prepare(
        input: TransactionSendInput,
        topic_id: TopicId,
        max_wire_batch_bytes: usize,
    ) -> Result<Self, TransactionSendInput> {
        let mut records = Vec::new();
        if records.try_reserve_exact(1).is_err() {
            return Err(input);
        }
        let TransactionSendInput {
            epoch,
            original_record,
            canonical_topic: topic,
            partition: source_partition,
            materialization_record,
            retained_source_bytes,
            deadline,
        } = input;
        records.push(materialization_record);
        let partition =
            source_partition.map(|partition| TransactionPartition::new(topic_id, partition));
        Ok(Self {
            epoch,
            original_record,
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

    pub(super) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) fn key_bytes(&self) -> Option<&[u8]> {
        self.records
            .first()
            .and_then(MaterializationRecord::key_bytes)
            .map(bytes::Bytes::as_ref)
    }

    pub(super) fn assign_partition(&mut self, partition: PartitionIndex) -> bool {
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

    pub(super) fn record_count(&self) -> usize {
        self.records.len()
    }

    pub(in crate::transaction) fn into_input(mut self) -> TransactionSendInput {
        let materialization_record = self
            .records
            .pop()
            .unwrap_or_else(|| unreachable!("resolved send retains one materialization record"));
        debug_assert!(self.records.is_empty());
        TransactionSendInput::new(
            self.epoch,
            self.original_record,
            self.topic,
            self.source_partition,
            materialization_record,
            self.retained_source_bytes,
            self.deadline,
        )
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        TransactionEpoch,
        TransactionPartition,
        Arc<str>,
        Vec<MaterializationRecord>,
        usize,
        OperationDeadline,
    ) {
        drop(self.original_record);
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

/// Local reason ownership never crossed deterministic send acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionSendAdmissionFailureKind {
    Busy,
    SendIdentityExhausted,
    InvalidPartition,
    Lifecycle(TransactionLifecycleHostError),
}

/// Exact unresolved input restored when no transactional send was accepted.
#[must_use = "rejected transactional send admission restores its exact input"]
#[derive(Debug)]
pub(crate) struct TransactionSendAdmissionFailure {
    kind: TransactionSendAdmissionFailureKind,
    request: TransactionSendRequest,
}

impl TransactionSendAdmissionFailure {
    pub(super) const fn new(
        kind: TransactionSendAdmissionFailureKind,
        request: TransactionSendRequest,
    ) -> Self {
        Self { kind, request }
    }

    pub(crate) const fn kind(&self) -> TransactionSendAdmissionFailureKind {
        self.kind
    }

    pub(in crate::transaction) fn into_input(self) -> TransactionSendInput {
        self.request.into_input()
    }
}
