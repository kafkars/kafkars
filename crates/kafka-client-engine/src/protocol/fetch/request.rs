//! One-partition name-based normal-consumer Fetch request construction.

use kafka_wire::{
    FetchRequest,
    fetch_request::{FetchPartition, FetchTopic},
};

use super::isolation::FetchIsolation;
use super::session::FetchSessionRequest;

/// First generated Fetch version that represents isolation and request max bytes.
pub(crate) const FETCH_NAME_ROUTE_MIN_VERSION: i16 = 4;

/// Last Fetch version that represents topics by name rather than topic ID.
///
/// This is a submission ceiling, not a selected version: driver negotiation
/// must choose an intersection within `4..=12`.
pub(crate) const FETCH_NAME_ROUTE_MAX_VERSION: i16 = 12;

const MAX_TOPIC_NAME_BYTES: usize = 249;
const CONSUMER_REPLICA_ID: i32 = -1;
const UNKNOWN_LEADER_EPOCH: i32 = -1;
const NO_LAST_FETCHED_EPOCH: i32 = -1;
const CONSUMER_LOG_START_OFFSET: i64 = -1;

/// Raw bounded settings compiled by a future direct-consumer interpreter.
///
/// Kafka's KIP-74 byte fields are soft broker limits for the first oversized
/// batch. Driver frame bounds and response decode budgets therefore remain
/// authoritative even after these values pass validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchRequestSettings {
    max_wait_ms: u32,
    min_bytes: u32,
    max_bytes: u32,
    partition_max_bytes: u32,
    isolation_level: i8,
}

impl FetchRequestSettings {
    pub(crate) const fn new(
        max_wait_ms: u32,
        min_bytes: u32,
        max_bytes: u32,
        partition_max_bytes: u32,
        isolation_level: i8,
    ) -> Self {
        Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            partition_max_bytes,
            isolation_level,
        }
    }

    /// Caps the broker long poll without changing any other request setting.
    pub(crate) const fn cap_max_wait_ms(self, limit: u32) -> Self {
        let max_wait_ms = if self.max_wait_ms < limit {
            self.max_wait_ms
        } else {
            limit
        };
        Self {
            max_wait_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            partition_max_bytes: self.partition_max_bytes,
            isolation_level: self.isolation_level,
        }
    }

    /// Replaces raw configuration with one core-selected isolation policy.
    pub(crate) const fn with_isolation(self, isolation: FetchIsolation) -> Self {
        Self {
            max_wait_ms: self.max_wait_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            partition_max_bytes: self.partition_max_bytes,
            isolation_level: isolation.wire_value(),
        }
    }

    /// Returns the closed isolation represented by validated settings.
    pub(crate) const fn isolation(self) -> Option<FetchIsolation> {
        match self.isolation_level {
            0 => Some(FetchIsolation::ReadUncommitted),
            1 => Some(FetchIsolation::ReadCommitted),
            _ => None,
        }
    }
}

/// Why one core-selected fetch could not become a generated request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRequestFailure {
    EmptyTopic,
    TopicTooLong { actual: usize, limit: usize },
    PartitionOutOfRange { actual: u32 },
    NegativeFetchOffset { actual: i64 },
    MaxWaitOutOfRange { actual: u32 },
    MinBytesOutOfRange { actual: u32 },
    MaxBytesOutOfRange { actual: u32 },
    PartitionMaxBytesOutOfRange { actual: u32 },
    MinBytesExceedMaxBytes { min_bytes: u32, max_bytes: u32 },
    InvalidIsolationLevel { actual: i8 },
}

/// Builds one legacy, no-session Fetch request for one name-routed partition.
///
/// Execution must negotiate a generated wire version in `4..=12`: v4 is the
/// first supported version that represents isolation and global `max_bytes`,
/// while v12 is the final version that still represents the topic name.
pub(crate) fn fetch_request(
    topic: &str,
    partition: u32,
    fetch_offset: i64,
    settings: FetchRequestSettings,
) -> Result<FetchRequest, FetchRequestFailure> {
    fetch_request_with_session(
        topic,
        partition,
        fetch_offset,
        settings,
        FetchSessionRequest::LEGACY,
    )
}

/// Builds one session-fenced Fetch request for one name-routed partition.
pub(crate) fn fetch_request_with_session(
    topic: &str,
    partition: u32,
    fetch_offset: i64,
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
) -> Result<FetchRequest, FetchRequestFailure> {
    validate_topic(topic)?;
    let partition = i32::try_from(partition)
        .map_err(|_| FetchRequestFailure::PartitionOutOfRange { actual: partition })?;
    if fetch_offset < 0 {
        return Err(FetchRequestFailure::NegativeFetchOffset {
            actual: fetch_offset,
        });
    }
    let max_wait_ms = positive_or_zero(settings.max_wait_ms, |actual| {
        FetchRequestFailure::MaxWaitOutOfRange { actual }
    })?;
    let min_bytes = positive_or_zero(settings.min_bytes, |actual| {
        FetchRequestFailure::MinBytesOutOfRange { actual }
    })?;
    let max_bytes = positive(settings.max_bytes, |actual| {
        FetchRequestFailure::MaxBytesOutOfRange { actual }
    })?;
    let partition_max_bytes = positive(settings.partition_max_bytes, |actual| {
        FetchRequestFailure::PartitionMaxBytesOutOfRange { actual }
    })?;
    if settings.min_bytes > settings.max_bytes {
        return Err(FetchRequestFailure::MinBytesExceedMaxBytes {
            min_bytes: settings.min_bytes,
            max_bytes: settings.max_bytes,
        });
    }
    if !matches!(settings.isolation_level, 0 | 1) {
        return Err(FetchRequestFailure::InvalidIsolationLevel {
            actual: settings.isolation_level,
        });
    }

    let mut generated_partition = FetchPartition::default();
    generated_partition.partition = partition;
    generated_partition.current_leader_epoch = UNKNOWN_LEADER_EPOCH;
    generated_partition.fetch_offset = fetch_offset;
    generated_partition.last_fetched_epoch = NO_LAST_FETCHED_EPOCH;
    generated_partition.log_start_offset = CONSUMER_LOG_START_OFFSET;
    generated_partition.partition_max_bytes = partition_max_bytes;

    let mut generated_topic = FetchTopic::default();
    generated_topic.topic = topic.into();
    generated_topic.partitions = vec![generated_partition];

    let mut request = FetchRequest::default();
    request.replica_id = CONSUMER_REPLICA_ID;
    request.max_wait_ms = max_wait_ms;
    request.min_bytes = min_bytes;
    request.max_bytes = max_bytes;
    request.isolation_level = settings.isolation_level;
    request.session_id = session.session_id();
    request.session_epoch = session.session_epoch();
    request.topics = vec![generated_topic];
    Ok(request)
}

fn validate_topic(topic: &str) -> Result<(), FetchRequestFailure> {
    if topic.is_empty() {
        return Err(FetchRequestFailure::EmptyTopic);
    }
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(FetchRequestFailure::TopicTooLong {
            actual: topic.len(),
            limit: MAX_TOPIC_NAME_BYTES,
        });
    }
    Ok(())
}

fn positive_or_zero(
    value: u32,
    failure: fn(u32) -> FetchRequestFailure,
) -> Result<i32, FetchRequestFailure> {
    i32::try_from(value).map_err(|_| failure(value))
}

fn positive(
    value: u32,
    failure: fn(u32) -> FetchRequestFailure,
) -> Result<i32, FetchRequestFailure> {
    let converted = positive_or_zero(value, failure)?;
    if converted == 0 {
        return Err(failure(value));
    }
    Ok(converted)
}
