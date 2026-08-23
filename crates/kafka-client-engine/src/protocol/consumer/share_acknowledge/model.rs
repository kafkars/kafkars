//! Bounded generated-free facts normalized from `ShareAcknowledge` v1.

use core::num::NonZeroI16;

use bytes::Bytes;

pub(crate) const SHARE_ACKNOWLEDGE_MIN_VERSION: i16 = 1;
pub(crate) const SHARE_ACKNOWLEDGE_MAX_VERSION: i16 = 1;
pub(super) const SHARE_ACKNOWLEDGE_MAX_TOPICS: usize = 64;
pub(super) const SHARE_ACKNOWLEDGE_MAX_PARTITIONS: usize = 64;
pub(super) const SHARE_ACKNOWLEDGE_MAX_BATCHES: usize = 4_096;
pub(super) const SHARE_ACKNOWLEDGE_MAX_ENDPOINTS: usize = 64;
pub(super) const SHARE_ACKNOWLEDGE_MAX_DIAGNOSTIC_BYTES: usize = 1_024;

/// One exact request topic-partition key.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ShareAcknowledgePartitionKey {
    pub(super) topic_id: [u8; 16],
    pub(super) partition: u32,
}

/// Complete canonical partition correlation retained beside one tracked call.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgeCorrelation {
    pub(super) partitions: Vec<ShareAcknowledgePartitionKey>,
}

impl ShareAcknowledgeCorrelation {
    pub(super) const fn new(partitions: Vec<ShareAcknowledgePartitionKey>) -> Self {
        Self { partitions }
    }

    pub(super) fn contains(&self, topic_id: [u8; 16], partition: u32) -> bool {
        self.partitions.contains(&ShareAcknowledgePartitionKey {
            topic_id,
            partition,
        })
    }
}

/// Exact nonzero top-level broker rejection.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgeBrokerRejection {
    pub(crate) throttle_time_ms: u32,
    pub(crate) error_code: NonZeroI16,
    pub(crate) error_message: Option<Bytes>,
}

/// Exactly correlated result for one requested partition.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgePartitionOutcome {
    pub(crate) topic_id: [u8; 16],
    pub(crate) partition: u32,
    pub(crate) error_code: Option<NonZeroI16>,
    pub(crate) error_message: Option<Bytes>,
    pub(crate) current_leader: Option<(i32, i32)>,
}

/// One validated current-leader endpoint retained for route refresh.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgeEndpoint {
    pub(crate) node_id: i32,
    pub(crate) host: Bytes,
    pub(crate) port: u16,
    pub(crate) rack: Option<Bytes>,
}

/// Successful top-level response with exact partition results.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareAcknowledgeSuccess {
    pub(crate) throttle_time_ms: u32,
    pub(crate) outcomes: Vec<ShareAcknowledgePartitionOutcome>,
    pub(crate) endpoints: Vec<ShareAcknowledgeEndpoint>,
}

/// Generated-free top-level `ShareAcknowledge` outcome.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareAcknowledgeOutcome {
    Succeeded(ShareAcknowledgeSuccess),
    Rejected(ShareAcknowledgeBrokerRejection),
}
