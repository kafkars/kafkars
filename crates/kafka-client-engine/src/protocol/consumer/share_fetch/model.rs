//! Bounded generated-free facts normalized from `ShareFetch` v1.

use core::num::NonZeroI16;

use bytes::Bytes;

use super::request_plan::ShareFetchRequestTopic;

pub(crate) const SHARE_FETCH_MIN_VERSION: i16 = 1;
pub(crate) const SHARE_FETCH_MAX_VERSION: i16 = 1;
pub(crate) const SHARE_FETCH_MAX_TOPICS: usize = 64;
pub(crate) const SHARE_FETCH_MAX_PARTITIONS: usize = 64;
pub(crate) const SHARE_FETCH_MAX_RANGES: usize = 4_096;
pub(crate) const SHARE_FETCH_MAX_ENDPOINTS: usize = 64;

/// Hard bounds applied before retaining one generated response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchResponseLimits {
    max_records: u64,
    max_retained_bytes: usize,
}

impl ShareFetchResponseLimits {
    pub(crate) const fn new(max_records: u64, max_retained_bytes: usize) -> Self {
        Self {
            max_records,
            max_retained_bytes,
        }
    }

    pub(super) const fn max_records(self) -> u64 {
        self.max_records
    }

    pub(super) const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

/// Complete active topic/partition correlation retained beside a tracked call.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchCorrelation {
    pub(super) topics: Vec<ShareFetchRequestTopic>,
}

impl ShareFetchCorrelation {
    pub(super) const fn new(topics: Vec<ShareFetchRequestTopic>) -> Self {
        Self { topics }
    }

    pub(crate) fn contains(&self, topic_id: [u8; 16], partition: u32) -> bool {
        self.topics
            .iter()
            .find(|candidate| candidate.topic_id == topic_id)
            .is_some_and(|candidate| candidate.partitions.contains(&partition))
    }

    pub(super) fn contains_topic(&self, topic_id: [u8; 16]) -> bool {
        self.topics
            .iter()
            .any(|candidate| candidate.topic_id == topic_id)
    }
}

/// Exact nonzero top-level broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchBrokerRejection {
    pub(crate) throttle_time_ms: u32,
    pub(crate) error_code: NonZeroI16,
}

/// One normalized acquired inclusive offset range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchAcquiredRange {
    pub(crate) first_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) delivery_count: i16,
}

/// Exact partition-level fetch and acknowledgement rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchPartitionRejection {
    pub(crate) fetch_error: Option<NonZeroI16>,
    pub(crate) acknowledge_error: Option<NonZeroI16>,
    pub(crate) current_leader: Option<(i32, i32)>,
}

/// One correlated partition result retaining raw record bytes for bounded decoding.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchPartition {
    pub(crate) partition: u32,
    pub(crate) rejection: Option<ShareFetchPartitionRejection>,
    pub(crate) records: Bytes,
    pub(crate) acquired: Vec<ShareFetchAcquiredRange>,
}

/// One correlated topic result in broker order.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchTopic {
    pub(crate) topic_id: [u8; 16],
    pub(crate) partitions: Vec<ShareFetchPartition>,
}

/// One validated current-leader endpoint retained for route refresh.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchEndpoint {
    pub(crate) node_id: i32,
    pub(crate) host: Bytes,
    pub(crate) port: u16,
    pub(crate) rack: Option<Bytes>,
}

/// Successful response facts before session and acquisition policy.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchSuccess {
    pub(crate) throttle_time_ms: u32,
    pub(crate) acquisition_lock_timeout_ms: Option<u32>,
    pub(crate) topics: Vec<ShareFetchTopic>,
    pub(crate) endpoints: Vec<ShareFetchEndpoint>,
    pub(crate) retained_records: u64,
    pub(crate) retained_bytes: usize,
}

/// Generated-free top-level `ShareFetch` outcome.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ShareFetchOutcome {
    Succeeded(ShareFetchSuccess),
    Rejected(ShareFetchBrokerRejection),
}
