//! Materialization access to admitted payloads without duplicating bytes.

use kafka_client_core::{ByteCount, PartitionIndex, PayloadId, TopicId};

use super::{
    ProducerRecord, ProducerStoreError,
    record_store::{PayloadState, RecordSlot, RecordStore},
};

impl RecordSlot {
    pub(super) fn commit_reservation(&mut self) {
        self.state = PayloadState::Admitted;
    }

    fn take_record(&mut self) -> Result<ProducerRecord, ProducerStoreError> {
        if self.state != PayloadState::Admitted {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        let record = self
            .record
            .take()
            .ok_or(ProducerStoreError::InvalidPayloadState)?;
        self.state = PayloadState::Materialized;
        Ok(record)
    }

    fn restore_record(&mut self, record: ProducerRecord) -> Result<(), ProducerStoreError> {
        if self.state != PayloadState::Materialized || self.record.is_some() {
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
        Ok((slot.topic_id, record.partition()))
    }

    pub(super) fn record(
        &self,
        payload_id: PayloadId,
    ) -> Result<&ProducerRecord, ProducerStoreError> {
        let slot = self.slot(payload_id)?;
        if slot.state == PayloadState::Reserved {
            return Err(ProducerStoreError::InvalidPayloadState);
        }
        slot.record
            .as_ref()
            .ok_or(ProducerStoreError::InvalidPayloadState)
    }

    pub(super) fn take_for_materialization(
        &mut self,
        payload_id: PayloadId,
    ) -> Result<ProducerRecord, ProducerStoreError> {
        let slot = self.slot_mut(payload_id)?;
        slot.take_record()
    }

    pub(super) fn restore_after_materialization(
        &mut self,
        payload_id: PayloadId,
        record: ProducerRecord,
    ) -> Result<(), ProducerStoreError> {
        let Some(slot) = self.slots.get_mut(&payload_id) else {
            return Err(ProducerStoreError::UnknownPayload);
        };
        slot.restore_record(record)
    }

    pub(super) const fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    #[cfg(test)]
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
