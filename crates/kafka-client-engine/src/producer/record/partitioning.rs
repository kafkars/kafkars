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

    pub(in crate::producer) fn assign_partition(&mut self, partition: PartitionIndex) -> bool {
        if self.partition.is_some() {
            return false;
        }
        self.partition = Some(partition);
        true
    }

    pub(in crate::producer) const fn is_automatic_unkeyed(&self) -> bool {
        self.automatic_partition && self.key.is_none()
    }

    pub(in crate::producer) fn into_public_parts(self) -> ProducerRecordParts {
        ProducerRecordParts {
            topic: self.topic,
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
        }
    }
}
