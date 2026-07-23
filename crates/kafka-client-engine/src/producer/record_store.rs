//! Sole owner of payload identities, record capacity, and retained byte counts.

use std::collections::BTreeMap;

use kafka_client_core::{ByteCount, ExplicitRecord, PayloadId, TopicId};

use super::{
    ProducerAdmissionError, ProducerRecord, ProducerStoreError, topic_catalog::TopicCatalog,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PayloadState {
    Reserved,
    Admitted,
    Materialized,
}

#[derive(Debug)]
pub(super) struct RecordSlot {
    pub(super) record: Option<ProducerRecord>,
    pub(super) retained_bytes: usize,
    pub(super) topic_id: TopicId,
    pub(super) state: PayloadState,
}

/// Linear proof that record bytes and count were reserved before core admission.
#[derive(Debug)]
pub(crate) struct RecordReservation {
    payload_id: PayloadId,
    facts: ExplicitRecord,
    record: ProducerRecord,
}

impl RecordReservation {
    /// Returns the bytes-free deterministic facts presented to core admission.
    pub(crate) const fn facts(&self) -> ExplicitRecord {
        self.facts
    }

    pub(super) fn into_parts(self) -> (PayloadId, ExplicitRecord, ProducerRecord) {
        (self.payload_id, self.facts, self.record)
    }
}

/// Exact record ownership paired with the result of reservation cleanup.
#[derive(Debug)]
pub(crate) struct RecordRollback {
    record: ProducerRecord,
    cleanup: Result<(), ProducerStoreError>,
}

impl RecordRollback {
    pub(super) const fn new(
        record: ProducerRecord,
        cleanup: Result<(), ProducerStoreError>,
    ) -> Self {
        Self { record, cleanup }
    }

    pub(crate) fn into_parts(self) -> (ProducerRecord, Result<(), ProducerStoreError>) {
        (self.record, self.cleanup)
    }
}

/// Bounded engine owner of application records.
#[derive(Debug)]
pub(super) struct RecordStore {
    pub(super) max_records: usize,
    pub(super) max_bytes: usize,
    pub(super) next_payload_id: Option<PayloadId>,
    pub(super) used_bytes: usize,
    pub(super) slots: BTreeMap<PayloadId, RecordSlot>,
    pub(super) topics: TopicCatalog,
}

impl RecordStore {
    pub(super) const fn new(max_records: usize, max_bytes: usize) -> Self {
        Self {
            max_records,
            max_bytes,
            next_payload_id: Some(PayloadId::from_raw(1)),
            used_bytes: 0,
            slots: BTreeMap::new(),
            topics: TopicCatalog::new(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "capacity rejection returns the intact linear record without allocating"
    )]
    pub(super) fn reserve(
        &mut self,
        record: ProducerRecord,
    ) -> Result<RecordReservation, ProducerAdmissionError> {
        let retained_bytes = match record.retained_bytes() {
            Ok(size) => size,
            Err(error) => return Err(ProducerAdmissionError::new(error, record)),
        };
        if self.slots.len() >= self.max_records {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::RecordCapacity,
                record,
            ));
        }
        let Some(next_used) = self.used_bytes.checked_add(retained_bytes) else {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::RetainedSizeOverflow,
                record,
            ));
        };
        if next_used > self.max_bytes {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::ByteCapacity,
                record,
            ));
        }
        let Some(payload_id) = self.next_payload_id else {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::PayloadIdentityExhausted,
                record,
            ));
        };
        if self.slots.contains_key(&payload_id) {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::PayloadIdentityExhausted,
                record,
            ));
        }
        let Ok(retained_u64) = u64::try_from(retained_bytes) else {
            return Err(ProducerAdmissionError::new(
                ProducerStoreError::RetainedSizeOverflow,
                record,
            ));
        };
        let topic = std::sync::Arc::clone(record.topic());
        let topic_id = match self.topics.acquire(topic) {
            Ok(id) => id,
            Err(error) => return Err(ProducerAdmissionError::new(error, record)),
        };
        let facts = ExplicitRecord::new(
            payload_id,
            topic_id,
            record.partition(),
            ByteCount::new(retained_u64),
        );
        self.next_payload_id = payload_id.get().checked_add(1).map(PayloadId::from_raw);
        self.used_bytes = next_used;
        self.slots.insert(
            payload_id,
            RecordSlot {
                record: None,
                retained_bytes,
                topic_id,
                state: PayloadState::Reserved,
            },
        );
        Ok(RecordReservation {
            payload_id,
            facts,
            record,
        })
    }

    pub(super) fn commit(
        &mut self,
        reservation: RecordReservation,
    ) -> Result<(), ProducerStoreError> {
        let (payload_id, _facts, record) = reservation.into_parts();
        self.slot_mut(payload_id)?.commit_reservation(record)
    }

    pub(super) fn rollback(&mut self, reservation: RecordReservation) -> RecordRollback {
        let (payload_id, _facts, record) = reservation.into_parts();
        let cleanup = self.rollback_slot(payload_id);
        RecordRollback::new(record, cleanup)
    }

    pub(super) fn release(
        &mut self,
        payload_id: PayloadId,
        expected: ByteCount,
    ) -> Result<(), ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        if slot.state == PayloadState::Reserved {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        if u64::try_from(slot.retained_bytes) != Ok(expected.get()) {
            return Err(ProducerStoreError::RetainedSizeMismatch);
        }
        let _released = self.remove_slot(payload_id)?;
        Ok(())
    }

    pub(super) fn clear_terminal(&mut self) {
        self.slots.clear();
        self.topics.clear_terminal();
        self.used_bytes = 0;
    }

    fn remove_slot(&mut self, payload_id: PayloadId) -> Result<RecordSlot, ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        let Some(next_used) = self.used_bytes.checked_sub(slot.retained_bytes) else {
            return Err(ProducerStoreError::InvalidPayloadState);
        };
        let topic_id = slot.topic_id;
        self.topics.release(topic_id)?;
        let Some(removed) = self.slots.remove(&payload_id) else {
            return Err(ProducerStoreError::UnknownPayload);
        };
        self.used_bytes = next_used;
        Ok(removed)
    }

    fn rollback_slot(&mut self, payload_id: PayloadId) -> Result<(), ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        if slot.state != PayloadState::Reserved || slot.record.is_some() {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        let _removed = self.remove_slot(payload_id)?;
        Ok(())
    }
}
