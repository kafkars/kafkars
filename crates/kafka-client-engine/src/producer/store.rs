//! Atomic coordination of record ownership and ordered batch membership.

mod batch_lifecycle;
#[cfg(test)]
mod batch_lifecycle_test;
mod materialization_view;
#[cfg(test)]
mod materialization_view_test;

use kafka_client_core::{
    ByteCount, PayloadId,
    partitioning::{PartitionSelection, StickyPartitionError, TopicPartitionFacts},
};

use super::{
    ProducerAdmissionError, ProducerRecord, ProducerStoreError, batch_store::BatchStore,
    record_store::RecordStore,
};

/// Hard bounds applied before work reaches deterministic core admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerStoreLimits {
    /// Maximum simultaneously retained application records.
    pub(crate) records: usize,
    /// Maximum bytes owned on behalf of retained application records.
    pub(crate) bytes: usize,
    /// Maximum simultaneous logical batch accumulators.
    pub(crate) batches: usize,
}

/// Current bounded producer ownership, excluding separately bounded encoded bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerStoreStats {
    /// Retained application record count.
    pub(crate) records: usize,
    /// Retained application bytes.
    pub(crate) bytes: usize,
    /// Retained logical batch count.
    pub(crate) batches: usize,
    /// Interned topic names retained by live records.
    pub(crate) topics: usize,
}

/// Linear engine owner of producer application bytes and batch membership.
#[derive(Debug)]
pub(crate) struct ProducerStore {
    records: RecordStore,
    batches: BatchStore,
}

impl ProducerStore {
    /// Creates an empty producer store with explicit count and byte bounds.
    pub(crate) const fn new(limits: ProducerStoreLimits) -> Self {
        Self::new_with_topic_limits(limits, limits.records, limits.bytes)
    }

    /// Creates a store whose producer-lifetime topic identities have independent bounds.
    pub(crate) const fn new_with_topic_limits(
        limits: ProducerStoreLimits,
        max_topics: usize,
        max_topic_bytes: usize,
    ) -> Self {
        Self {
            records: RecordStore::new_with_topic_limits(
                limits.records,
                limits.bytes,
                max_topics,
                max_topic_bytes,
            ),
            batches: BatchStore::new(limits.batches),
        }
    }

    /// Reserves count, bytes, payload identity, and topic identity before core.
    #[allow(
        clippy::result_large_err,
        reason = "capacity rejection returns the intact linear record without allocating"
    )]
    pub(crate) fn reserve(
        &mut self,
        record: ProducerRecord,
    ) -> Result<RecordReservation, ProducerAdmissionError> {
        self.records.reserve(record)
    }

    /// Commits a reservation only after deterministic core accepted its facts.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the reservation is a linear core-admission capability"
    )]
    pub(crate) fn commit(
        &mut self,
        reservation: RecordReservation,
    ) -> Result<kafka_client_core::ExplicitRecord, ProducerStoreError> {
        let facts = reservation.facts();
        self.records.commit(reservation)?;
        Ok(facts)
    }

    /// Atomically returns the intact record when core rejects its reservation.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "rollback consumes the linear pre-core reservation"
    )]
    pub(crate) fn rollback(
        &mut self,
        reservation: RecordReservation,
    ) -> super::record_store::RecordRollback {
        self.records.rollback(reservation)
    }

    /// Releases one admitted payload exactly once with provenance checking.
    pub(crate) fn release_payload(
        &mut self,
        payload_id: PayloadId,
        expected: ByteCount,
    ) -> Result<(), ProducerStoreError> {
        if self.batches.contains_payload(payload_id) {
            return Err(ProducerStoreError::PayloadStillBatched);
        }
        self.records.release(payload_id, expected)
    }

    /// Returns current count and byte ownership for metrics and tests.
    pub(crate) fn stats(&self) -> ProducerStoreStats {
        ProducerStoreStats {
            records: self.records.len(),
            bytes: self.records.used_bytes(),
            batches: self.batches.len(),
            topics: self.records.topic_count(),
        }
    }

    /// Drops all engine-owned records and batch membership terminally.
    pub(crate) fn clear_terminal(&mut self) {
        self.batches.clear_terminal();
        self.records.clear_terminal();
    }

    pub(super) fn topic_count(&self) -> usize {
        self.records.topic_count()
    }

    pub(super) fn retain_waiting_topic(
        &mut self,
        topic: std::sync::Arc<str>,
    ) -> Result<kafka_client_core::TopicId, ProducerStoreError> {
        self.records.topics.acquire(topic)
    }

    pub(super) fn release_waiting_topic(
        &mut self,
        topic_id: kafka_client_core::TopicId,
    ) -> Result<(), ProducerStoreError> {
        self.records.topics.release(topic_id)
    }

    pub(in crate::producer) fn select_sticky_partition(
        &mut self,
        topic_id: kafka_client_core::TopicId,
        facts: TopicPartitionFacts<'_>,
    ) -> Result<Result<PartitionSelection, StickyPartitionError>, ProducerStoreError> {
        self.records.topics.select_sticky(topic_id, facts)
    }
}

pub(crate) use super::record_store::RecordReservation;
