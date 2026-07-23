//! Atomic coordination of record ownership and ordered batch membership.

mod materialize;

use kafka_client_core::{BatchId, ByteCount, OperationId, PayloadId};

use super::{
    ProducerAdmissionError, ProducerRecord, ProducerStoreError,
    batch_store::{BatchRoute, BatchStore},
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
        Self {
            records: RecordStore::new(limits.records, limits.bytes),
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
        self.records.commit(&reservation)?;
        Ok(reservation.facts())
    }

    /// Atomically returns the intact record when core rejects its reservation.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "rollback consumes the linear pre-core reservation"
    )]
    pub(crate) fn rollback(
        &mut self,
        reservation: RecordReservation,
    ) -> Result<ProducerRecord, ProducerStoreError> {
        self.records.rollback(&reservation)
    }

    /// Appends a core-accepted operation to one ordered logical batch.
    pub(crate) fn accumulate(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
        payload_id: PayloadId,
    ) -> Result<ByteCount, ProducerStoreError> {
        let (topic_id, partition) = self.records.route(payload_id)?;
        let retained = self.records.retained_bytes(payload_id)?;
        self.batches.append(
            batch_id,
            operation_id,
            payload_id,
            BatchRoute {
                topic_id,
                partition,
            },
        )?;
        Ok(retained)
    }

    /// Removes one open-batch member while preserving all sibling order.
    pub(crate) fn remove_member(
        &mut self,
        batch_id: BatchId,
        operation_id: OperationId,
    ) -> Result<PayloadId, ProducerStoreError> {
        self.batches.remove_member(batch_id, operation_id)
    }

    /// Releases logical batch membership exactly once before payload release.
    pub(crate) fn release_batch(&mut self, batch_id: BatchId) -> Result<(), ProducerStoreError> {
        let _members = self.batches.release(batch_id)?;
        Ok(())
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
        }
    }

    #[cfg(test)]
    pub(super) fn topic_count(&self) -> usize {
        self.records.topic_count()
    }
}

pub(crate) use super::record_store::RecordReservation;
