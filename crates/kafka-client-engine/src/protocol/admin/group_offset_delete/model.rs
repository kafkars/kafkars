//! Borrowed request targets and validated response facts without generated ownership.

use core::num::NonZeroI16;

/// One caller-owned topic-partition target borrowed while building or correlating a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OffsetDeleteTargetRef<'a> {
    topic: &'a str,
    partition: i32,
}

impl<'a> OffsetDeleteTargetRef<'a> {
    pub(crate) const fn new(topic: &'a str, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }
}

/// Exact partition-level broker result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OffsetDeletePartitionResult {
    Deleted,
    Rejected { code: NonZeroI16 },
}

/// One generated response fact correlated back to caller order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OffsetDeletePartitionRef<'a> {
    topic: &'a str,
    partition: i32,
    result: OffsetDeletePartitionResult,
    caller_index: usize,
}

impl<'a> OffsetDeletePartitionRef<'a> {
    pub(super) const fn new(
        topic: &'a str,
        partition: i32,
        result: OffsetDeletePartitionResult,
        caller_index: usize,
    ) -> Self {
        Self {
            topic,
            partition,
            result,
            caller_index,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn result(self) -> OffsetDeletePartitionResult {
        self.result
    }

    pub(super) const fn caller_index(self) -> usize {
        self.caller_index
    }
}

/// Borrowed, caller-ordered facts whose future owned allocation charge is proven.
#[must_use = "validated offset-deletion facts must be terminally interpreted"]
pub(crate) struct ValidatedOffsetDeleteResponse<'a> {
    entries: Vec<OffsetDeletePartitionRef<'a>>,
    throttle_time_ms: u32,
    top_level_error: Option<NonZeroI16>,
    retained_charge: usize,
}

impl<'a> ValidatedOffsetDeleteResponse<'a> {
    pub(super) const fn new(
        entries: Vec<OffsetDeletePartitionRef<'a>>,
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

    pub(crate) const fn retained_charge(&self) -> usize {
        self.retained_charge
    }

    pub(crate) fn entries(&self) -> &[OffsetDeletePartitionRef<'a>] {
        &self.entries
    }

    pub(crate) fn into_validated_deletions(self) -> Vec<OffsetDeletePartitionRef<'a>> {
        self.entries
    }
}
