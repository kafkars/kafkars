//! Automatic partition state retained with one producer record.

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::{ProducerRecord, ProducerRecordParts};

impl ProducerRecord {
    pub(in crate::producer) const fn selected_partition(&self) -> Option<PartitionIndex> {
        self.partition
    }

    pub(in crate::producer) const fn needs_partition(&self) -> bool {
        self.partition.is_none()
    }

    pub(in crate::producer) fn key_bytes(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    pub(in crate::producer) fn assign_partition(
        &mut self,
        partition: PartitionIndex,
        leader_broker_id: Option<i32>,
    ) -> bool {
        if self.partition.is_some() {
            return false;
        }
        self.partition = Some(partition);
        self.leader_broker_id = leader_broker_id;
        true
    }

    pub(in crate::producer) const fn leader_broker_id(&self) -> Option<i32> {
        self.leader_broker_id
    }

    pub(in crate::producer) fn update_partition_leader(&mut self, leader_broker_id: Option<i32>) {
        debug_assert!(self.partition.is_some());
        self.leader_broker_id = leader_broker_id;
    }

    pub(in crate::producer) const fn is_automatic_unkeyed(&self) -> bool {
        self.automatic_partition && self.key.is_none()
    }

    pub(in crate::producer) fn into_public_parts(self) -> ProducerRecordParts {
        ProducerRecordParts {
            topic: self.topic,
            expected_topic_uuid: self.expected_topic_uuid,
            partition: if self.automatic_partition {
                None
            } else {
                self.partition
            },
            timestamp_ms: self.timestamp_ms,
            defaulted_timestamp: self.defaulted_timestamp,
            key: self.key,
            value: self.value,
            headers: self.headers,
            source_owner: self.source_owner,
        }
    }
}
