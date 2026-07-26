//! Scalar borrowed offset facts retained without generated-message ownership.

use core::{cmp::Ordering, num::NonZeroI16};

/// One successful or rejected committed-offset value borrowed from the wire DTO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetValueRef<'a> {
    /// Kafka returned a committed-offset fact for this partition.
    Committed {
        /// `None` is Kafka's `-1` no-committed-offset sentinel.
        offset: Option<i64>,
        /// Present only when the selected response version carries leader epochs.
        leader_epoch: Option<i32>,
        /// Nullable application-owned commit metadata.
        metadata: Option<&'a str>,
    },
    /// Kafka rejected this partition with an exact signed nonzero code.
    Rejected {
        /// Exact code, including values unknown to this client revision.
        code: NonZeroI16,
    },
}

/// One topic-partition fact borrowed from a validated generated response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BorrowedGroupOffset<'a> {
    topic: &'a str,
    partition: i32,
    value: GroupOffsetValueRef<'a>,
    source_topic: usize,
}

impl<'a> BorrowedGroupOffset<'a> {
    pub(super) const fn new(
        topic: &'a str,
        partition: i32,
        value: GroupOffsetValueRef<'a>,
        source_topic: usize,
    ) -> Self {
        Self {
            topic,
            partition,
            value,
            source_topic,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn value(self) -> GroupOffsetValueRef<'a> {
        self.value
    }

    pub(super) const fn source_topic(self) -> usize {
        self.source_topic
    }
}

pub(super) fn group_offset_order(
    left: &BorrowedGroupOffset<'_>,
    right: &BorrowedGroupOffset<'_>,
) -> Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
}

/// Borrowed sorted facts whose complete simultaneous allocation charge is proven.
#[must_use = "validated response facts must be terminally interpreted"]
pub(crate) struct ValidatedGroupOffsetsResponse<'a> {
    entries: Vec<BorrowedGroupOffset<'a>>,
    throttle_time_ms: u32,
    top_level_error: Option<NonZeroI16>,
    retained_charge: usize,
}

impl<'a> ValidatedGroupOffsetsResponse<'a> {
    pub(super) const fn new(
        entries: Vec<BorrowedGroupOffset<'a>>,
        throttle_time_ms: u32,
        top_level_error: Option<NonZeroI16>,
        retained_charge: usize,
    ) -> Self {
        Self {
            entries,
            throttle_time_ms,
            top_level_error,
            retained_charge,
        }
    }

    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn top_level_error(&self) -> Option<NonZeroI16> {
        self.top_level_error
    }

    pub(crate) const fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub(crate) const fn retained_charge(&self) -> usize {
        self.retained_charge
    }

    /// Borrows facts already sorted by topic UTF-8 bytes and partition.
    pub(crate) fn entries(&self) -> &[BorrowedGroupOffset<'a>] {
        &self.entries
    }

    /// Transfers charged temporary sort storage to the host boundary.
    pub(crate) fn into_validated_offsets(self) -> Vec<BorrowedGroupOffset<'a>> {
        self.entries
    }
}

pub(super) fn value_ref(
    error_code: i16,
    committed_offset: i64,
    committed_leader_epoch: i32,
    metadata: Option<&str>,
    selected_version: i16,
) -> GroupOffsetValueRef<'_> {
    match NonZeroI16::new(error_code) {
        Some(code) => GroupOffsetValueRef::Rejected { code },
        None => GroupOffsetValueRef::Committed {
            offset: (committed_offset != -1).then_some(committed_offset),
            leader_epoch: (selected_version >= 5 && committed_leader_epoch != -1)
                .then_some(committed_leader_epoch),
            metadata,
        },
    }
}
