//! Bounded engine-owned facts normalized from `ShareGroupHeartbeat` v1.

use core::num::NonZeroI16;
use std::sync::Arc;

pub(crate) const SHARE_GROUP_HEARTBEAT_MIN_VERSION: i16 = 1;
pub(crate) const SHARE_GROUP_HEARTBEAT_MAX_VERSION: i16 = 1;
pub(crate) const SHARE_GROUP_HEARTBEAT_MAX_TOPICS: usize = 64;
pub(crate) const SHARE_GROUP_HEARTBEAT_MAX_PARTITIONS: usize = 4_096;
pub(crate) const SHARE_GROUP_HEARTBEAT_MAX_TOPIC_BYTES: usize = 249;
pub(crate) const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;

/// One wire topic UUID and its ordered assigned partitions.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareGroupHeartbeatAssignmentTopic {
    topic_id: [u8; 16],
    partitions: Vec<u32>,
}

impl ShareGroupHeartbeatAssignmentTopic {
    pub(super) const fn new(topic_id: [u8; 16], partitions: Vec<u32>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }

    pub(crate) const fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    pub(crate) fn partitions(&self) -> &[u32] {
        &self.partitions
    }
}

/// Successful v1 heartbeat facts before deterministic membership policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareGroupHeartbeatSuccess {
    throttle_time_ms: u32,
    member_id: Option<Arc<str>>,
    member_epoch: i32,
    heartbeat_interval_ms: u32,
    assignment: Option<Vec<ShareGroupHeartbeatAssignmentTopic>>,
}

impl ShareGroupHeartbeatSuccess {
    pub(super) const fn new(
        throttle_time_ms: u32,
        member_id: Option<Arc<str>>,
        member_epoch: i32,
        heartbeat_interval_ms: u32,
        assignment: Option<Vec<ShareGroupHeartbeatAssignmentTopic>>,
    ) -> Self {
        Self {
            throttle_time_ms,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            assignment,
        }
    }

    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) fn member_id(&self) -> Option<&Arc<str>> {
        self.member_id.as_ref()
    }

    pub(crate) const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    pub(crate) const fn heartbeat_interval_ms(&self) -> u32 {
        self.heartbeat_interval_ms
    }

    pub(crate) fn assignment(&self) -> Option<&[ShareGroupHeartbeatAssignmentTopic]> {
        self.assignment.as_deref()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        Option<Arc<str>>,
        i32,
        u32,
        Option<Vec<ShareGroupHeartbeatAssignmentTopic>>,
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

/// Exact nonzero broker rejection with its quota delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareGroupHeartbeatBrokerRejection {
    throttle_time_ms: u32,
    error_code: NonZeroI16,
}

impl ShareGroupHeartbeatBrokerRejection {
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

/// Generated-free top-level `ShareGroupHeartbeat` outcome.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatOutcome {
    Succeeded(ShareGroupHeartbeatSuccess),
    Rejected(ShareGroupHeartbeatBrokerRejection),
}
