//! Materialization access to admitted payloads without duplicating bytes.

use kafka_client_core::{ByteCount, PartitionIndex, PayloadId, TopicId};

use super::{
    ProducerRecord, ProducerStoreError,
    record_store::{PayloadState, RecordSlot, RecordStore},
};

impl RecordSlot {
    pub(super) fn commit_reservation(
        &mut self,
        record: ProducerRecord,
    ) -> Result<(), ProducerStoreError> {
        if self.state != PayloadState::Reserved || self.record.is_some() {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        self.record = Some(record);
        self.state = PayloadState::Admitted;
        Ok(())
    }
}

impl RecordStore {
    pub(super) fn retained_bytes(
        &self,
        payload_id: PayloadId,
    ) -> Result<ByteCount, ProducerStoreError> {
        let retained = self.slot(payload_id)?.retained_bytes;
        let value =
            u64::try_from(retained).map_err(|_| ProducerStoreError::RetainedSizeOverflow)?;
        Ok(ByteCount::new(value))
    }

    pub(super) fn route(
        &self,
        payload_id: PayloadId,
    ) -> Result<(TopicId, PartitionIndex), ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        let record = self.record(payload_id)?;
        let partition = record
            .selected_partition()
            .ok_or(ProducerStoreError::InvalidPayloadState)?;
        Ok((slot.topic_id, partition))
    }

    pub(super) fn record(
        &self,
        payload_id: PayloadId,
    ) -> Result<&ProducerRecord, ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        if slot.state != PayloadState::Admitted {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        slot.record
            .as_ref()
            .ok_or(ProducerStoreError::InvalidPayloadState)
    }

    pub(super) const fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(super) fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub(super) fn slot(
        &self,
        payload_id: PayloadId,
    ) -> Result<&super::record_store::RecordSlot, ProducerStoreError> {
        self.slots
            .get(&payload_id)
            .ok_or(ProducerStoreError::UnknownPayload)
    }

    pub(super) fn slot_mut(
        &mut self,
        payload_id: PayloadId,
    ) -> Result<&mut super::record_store::RecordSlot, ProducerStoreError> {
        self.slots
            .get_mut(&payload_id)
            .ok_or(ProducerStoreError::UnknownPayload)
    }
}
