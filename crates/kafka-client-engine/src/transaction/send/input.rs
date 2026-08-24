//! Linear unresolved ingress and one resolved fixed-send request.

use std::sync::Arc;

use kafka_client_core::{PartitionIndex, TopicId, TransactionEpoch, TransactionPartition};

use crate::{
    clock::OperationDeadline,
    producer::{PublicProducerRecord, materialization::MaterializationRecord},
    transaction::TransactionLifecycleHostError,
};

mod request;

/// Exact caller input retained until deterministic send acceptance.
#[must_use = "transactional send input must be accepted or returned intact"]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionSendInput {
    epoch: TransactionEpoch,
    original_records: Vec<PublicProducerRecord>,
    canonical_topic: Arc<str>,
    partition: Option<PartitionIndex>,
    materialization_records: Vec<MaterializationRecord>,
    retained_source_bytes: usize,
    deadline: OperationDeadline,
}

impl TransactionSendInput {
    #[expect(
        clippy::result_large_err,
        reason = "single-record allocation failure returns the exact caller-owned record"
    )]
    pub(crate) fn try_new(
        epoch: TransactionEpoch,
        original_record: PublicProducerRecord,
        canonical_topic: Arc<str>,
        partition: Option<PartitionIndex>,
        materialization_record: MaterializationRecord,
        retained_source_bytes: usize,
        deadline: OperationDeadline,
    ) -> Result<Self, PublicProducerRecord> {
        let mut original_records = Vec::new();
        if original_records.try_reserve_exact(1).is_err() {
            return Err(original_record);
        }
        original_records.push(original_record);
        let mut materialization_records = Vec::new();
        if materialization_records.try_reserve_exact(1).is_err() {
            return Err(original_records
                .pop()
                .unwrap_or_else(|| unreachable!("single input retains its original record")));
        }
        materialization_records.push(materialization_record);
        Ok(Self {
            epoch,
            original_records,
            canonical_topic,
            partition,
            materialization_records,
            retained_source_bytes,
            deadline,
        })
    }

    pub(crate) fn new_batch(
        epoch: TransactionEpoch,
        original_records: Vec<PublicProducerRecord>,
        canonical_topic: Arc<str>,
        partition: PartitionIndex,
        materialization_records: Vec<MaterializationRecord>,
        retained_source_bytes: usize,
        deadline: OperationDeadline,
    ) -> Self {
        debug_assert!(!original_records.is_empty());
        debug_assert_eq!(original_records.len(), materialization_records.len());
        Self {
            epoch,
            original_records,
            canonical_topic,
            partition: Some(partition),
            materialization_records,
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

    pub(crate) fn record_count(&self) -> usize {
        self.original_records.len()
    }

    pub(crate) fn into_original_record(self) -> PublicProducerRecord {
        let mut records = self.into_original_records();
        let record = records
            .pop()
            .unwrap_or_else(|| unreachable!("single send retains one original record"));
        debug_assert!(records.is_empty());
        record
    }

    pub(crate) fn into_original_records(self) -> Vec<PublicProducerRecord> {
        self.original_records
    }
}

/// One catalog-resolved request before partition and sequence transfer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TransactionSendRequest {
    epoch: TransactionEpoch,
    original_records: Vec<PublicProducerRecord>,
    source_partition: Option<PartitionIndex>,
    topic_id: TopicId,
    partition: Option<TransactionPartition>,
    topic: Arc<str>,
    records: Vec<MaterializationRecord>,
    retained_source_bytes: usize,
    max_wire_batch_bytes: usize,
    deadline: OperationDeadline,
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
