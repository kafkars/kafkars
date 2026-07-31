//! Generated-type-free facts crossing the KIP-848 heartbeat protocol seam.

use core::num::NonZeroI16;
use std::sync::Arc;

/// First beta API floor for broker-owned consumer-group membership.
pub(crate) const CONSUMER_GROUP_HEARTBEAT_MIN_VERSION: i16 = 0;
/// First beta API ceiling; v1 client-generated member identity is a later additive slice.
pub(crate) const CONSUMER_GROUP_HEARTBEAT_MAX_VERSION: i16 = 0;
pub(super) const CONSUMER_GROUP_HEARTBEAT_MAX_TOPICS: usize = 64;
pub(super) const CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS: usize = 64;
pub(super) const CONSUMER_GROUP_HEARTBEAT_MAX_TOPIC_BYTES: usize = 249;
pub(super) const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;

/// One Kafka topic identity and its currently owned partitions.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupHeartbeatOwnedTopic {
    topic_id: [u8; 16],
    partitions: Vec<u32>,
}

impl ConsumerGroupHeartbeatOwnedTopic {
    /// Retains one already-bounded topic identity and partition set.
    pub(crate) const fn new(topic_id: [u8; 16], partitions: Vec<u32>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }

    pub(super) const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    pub(super) fn partitions(&self) -> &[u32] {
        &self.partitions
    }
}

/// One normalized assignment topic before engine-catalog identity translation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupHeartbeatAssignmentTopic {
    topic_id: [u8; 16],
    partitions: Vec<u32>,
}

impl ConsumerGroupHeartbeatAssignmentTopic {
    pub(crate) const fn new(topic_id: [u8; 16], partitions: Vec<u32>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }

    /// Returns the exact nonzero Kafka topic UUID bytes.
    pub(crate) const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns the canonical partition indexes assigned for this topic.
    pub(crate) fn partitions(&self) -> &[u32] {
        &self.partitions
    }
}

/// Exact broker rejection retained without retry classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupHeartbeatBrokerRejection {
    throttle_time_ms: u32,
    error_code: NonZeroI16,
}

impl ConsumerGroupHeartbeatBrokerRejection {
    pub(super) const fn new(throttle_time_ms: u32, error_code: NonZeroI16) -> Self {
        Self {
            throttle_time_ms,
            error_code,
        }
    }

    pub(crate) const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn error_code(self) -> NonZeroI16 {
        self.error_code
    }
}

/// Successful API 68 facts without membership or catalog policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupHeartbeatSuccess {
    throttle_time_ms: u32,
    member_id: Option<Arc<str>>,
    member_epoch: i32,
    heartbeat_interval_ms: u32,
    assignment: Option<Vec<ConsumerGroupHeartbeatAssignmentTopic>>,
}

impl ConsumerGroupHeartbeatSuccess {
    pub(super) const fn new(
        throttle_time_ms: u32,
        member_id: Option<Arc<str>>,
        member_epoch: i32,
        heartbeat_interval_ms: u32,
        assignment: Option<Vec<ConsumerGroupHeartbeatAssignmentTopic>>,
    ) -> Self {
        Self {
            throttle_time_ms,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            assignment,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        Option<Arc<str>>,
        i32,
        u32,
        Option<Vec<ConsumerGroupHeartbeatAssignmentTopic>>,
    ) {
        (
            self.throttle_time_ms,
            self.member_id,
            self.member_epoch,
            self.heartbeat_interval_ms,
            self.assignment,
        )
    }
}

/// One exact API 68 terminal without recovery policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupHeartbeatOutcome {
    Rejected(ConsumerGroupHeartbeatBrokerRejection),
    Succeeded(ConsumerGroupHeartbeatSuccess),
}
