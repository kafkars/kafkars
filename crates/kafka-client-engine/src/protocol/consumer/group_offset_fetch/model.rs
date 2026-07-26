//! Protocol-facing assignment correlation and borrowed committed-offset facts.

use std::sync::Arc;

use core::num::NonZeroI16;

/// One caller-ordered topic and its caller-ordered assigned partitions.
#[derive(Debug)]
pub(crate) struct GroupOffsetFetchTopic {
    name: Arc<str>,
    partition_indexes: Vec<i32>,
}

impl GroupOffsetFetchTopic {
    pub(crate) const fn new(name: Arc<str>, partition_indexes: Vec<i32>) -> Self {
        Self {
            name,
            partition_indexes,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn partition_indexes(&self) -> &[i32] {
        &self.partition_indexes
    }
}

/// Exact group spelling and assignment order retained apart from driver input.
#[must_use = "request correlation must be retained through response normalization"]
#[derive(Debug)]
pub(crate) struct GroupOffsetFetchCorrelation {
    group_id: Arc<str>,
    topics: Vec<GroupOffsetFetchTopic>,
    partition_count: usize,
}

impl GroupOffsetFetchCorrelation {
    pub(super) const fn new(
        group_id: Arc<str>,
        topics: Vec<GroupOffsetFetchTopic>,
        partition_count: usize,
    ) -> Self {
        Self {
            group_id,
            topics,
            partition_count,
        }
    }

    pub(crate) fn group_id(&self) -> &str {
        &self.group_id
    }

    pub(crate) fn topics(&self) -> &[GroupOffsetFetchTopic] {
        &self.topics
    }

    pub(crate) const fn partition_count(&self) -> usize {
        self.partition_count
    }
}

/// One exact partition response in caller assignment order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetFetchPartitionValueRef<'a> {
    /// Kafka supplied committed-position state for the assigned partition.
    Fetched {
        /// `None` is exactly Kafka's committed-offset `-1` sentinel.
        committed_offset: Option<i64>,
        /// `None` is absence before v5 or exactly Kafka's `-1` sentinel.
        committed_leader_epoch: Option<i32>,
        /// Nullable application-owned commit metadata.
        metadata: Option<&'a str>,
    },
    /// Kafka rejected the partition with an exact signed nonzero code.
    Rejected {
        /// Exact signed Kafka code, including unknown future values.
        code: NonZeroI16,
    },
}

/// Validated response facts whose entries follow caller assignment order.
#[must_use = "normalized group offsets must be bound to their retained correlation"]
pub(crate) struct NormalizedGroupOffsetFetch<'a> {
    throttle_time_ms: u32,
    top_level_error: Option<NonZeroI16>,
    entries: Vec<GroupOffsetFetchPartitionValueRef<'a>>,
    retained_charge: usize,
}

impl<'a> NormalizedGroupOffsetFetch<'a> {
    pub(super) const fn new(
        throttle_time_ms: u32,
        top_level_error: Option<NonZeroI16>,
        entries: Vec<GroupOffsetFetchPartitionValueRef<'a>>,
        retained_charge: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            top_level_error,
            entries,
            retained_charge,
        }
    }

    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn top_level_error(&self) -> Option<NonZeroI16> {
        self.top_level_error
    }

    pub(crate) fn entries(&self) -> &[GroupOffsetFetchPartitionValueRef<'a>] {
        &self.entries
    }

    pub(crate) const fn retained_charge(&self) -> usize {
        self.retained_charge
    }
}
