//! Linear acknowledgement capability and normalized wire-independent values.

use crate::AssignedTopicPartition;

use super::super::{
    ShareAcquisition, ShareAcquisitionGeneration, ShareFetchSessionFence, ShareTopicUuid,
};

/// Public application disposition for one acquired record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareDisposition {
    /// Processing succeeded and normal redelivery must stop.
    Accept,
    /// Processing may succeed later and the record should become available.
    Release,
    /// Processing is permanently rejected and normal redelivery must stop.
    Reject,
}

/// One decision correlated to an exact acquisition generation and record offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareRecordDecision {
    acquisition: ShareAcquisitionGeneration,
    offset: i64,
    disposition: ShareDisposition,
}

impl ShareRecordDecision {
    /// Creates one record decision for later exact-batch validation.
    pub const fn new(
        acquisition: ShareAcquisitionGeneration,
        offset: i64,
        disposition: ShareDisposition,
    ) -> Self {
        Self {
            acquisition,
            offset,
            disposition,
        }
    }

    /// Returns the exact acquisition generation.
    pub const fn acquisition(self) -> ShareAcquisitionGeneration {
        self.acquisition
    }

    /// Returns the absolute Kafka log offset.
    pub const fn offset(self) -> i64 {
        self.offset
    }

    /// Returns the caller's public disposition.
    pub const fn disposition(self) -> ShareDisposition {
        self.disposition
    }
}

/// Normalized v1 acknowledgement value; `Gap` never crosses the user facade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgeType {
    /// No application record was present at this acquired offset.
    Gap,
    /// Accept the acquired record.
    Accept,
    /// Release the acquired record for redelivery.
    Release,
    /// Reject the acquired record from normal redelivery.
    Reject,
}

impl ShareAcknowledgeType {
    /// Returns the exact Kafka v1 wire value.
    pub const fn wire_value(self) -> i8 {
        match self {
            Self::Gap => 0,
            Self::Accept => 1,
            Self::Release => 2,
            Self::Reject => 3,
        }
    }
}

impl From<ShareDisposition> for ShareAcknowledgeType {
    fn from(value: ShareDisposition) -> Self {
        match value {
            ShareDisposition::Accept => Self::Accept,
            ShareDisposition::Release => Self::Release,
            ShareDisposition::Reject => Self::Reject,
        }
    }
}

/// One ascending, nonoverlapping wire acknowledgement batch.
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcknowledgementBatch {
    topic_uuid: ShareTopicUuid,
    partition: AssignedTopicPartition,
    first_offset: i64,
    last_offset: i64,
    types: Vec<ShareAcknowledgeType>,
}

impl ShareAcknowledgementBatch {
    pub(super) const fn new(
        topic_uuid: ShareTopicUuid,
        partition: AssignedTopicPartition,
        first_offset: i64,
        last_offset: i64,
        types: Vec<ShareAcknowledgeType>,
    ) -> Self {
        Self {
            topic_uuid,
            partition,
            first_offset,
            last_offset,
            types,
        }
    }

    /// Returns the exact Kafka topic UUID.
    pub const fn topic_uuid(&self) -> ShareTopicUuid {
        self.topic_uuid
    }

    /// Returns the engine catalog topic and partition.
    pub const fn partition(&self) -> AssignedTopicPartition {
        self.partition
    }

    /// Returns the inclusive first offset.
    pub const fn first_offset(&self) -> i64 {
        self.first_offset
    }

    /// Returns the inclusive last offset.
    pub const fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns one value for the whole range or one value per acquired offset.
    pub fn acknowledge_types(&self) -> &[ShareAcknowledgeType] {
        &self.types
    }
}

/// One exact linear batch capability prepared for `ShareAcknowledge` v1.
#[must_use = "a share acknowledgement must be admitted or abandoned exactly once"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcknowledgement {
    fence: ShareFetchSessionFence,
    acquisitions: Vec<ShareAcquisition>,
    batches: Vec<ShareAcknowledgementBatch>,
}

impl ShareAcknowledgement {
    pub(super) const fn new(
        fence: ShareFetchSessionFence,
        acquisitions: Vec<ShareAcquisition>,
        batches: Vec<ShareAcknowledgementBatch>,
    ) -> Self {
        Self {
            fence,
            acquisitions,
            batches,
        }
    }

    /// Returns the acquisition-time broker-session fence.
    pub const fn fence(&self) -> ShareFetchSessionFence {
        self.fence
    }

    /// Returns every exact acquisition still consumed by this capability.
    pub fn acquisitions(&self) -> &[ShareAcquisition] {
        &self.acquisitions
    }

    /// Returns deterministic topic, partition, and offset ordered batches.
    pub fn batches(&self) -> &[ShareAcknowledgementBatch] {
        &self.batches
    }

    /// Recovers the exact acquisitions for admission or abandonment settlement.
    pub fn into_parts(self) -> (Vec<ShareAcquisition>, Vec<ShareAcknowledgementBatch>) {
        (self.acquisitions, self.batches)
    }
}
