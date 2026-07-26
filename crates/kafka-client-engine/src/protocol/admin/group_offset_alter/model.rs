//! Borrowed offset-alteration intent and validated response facts.

use core::num::NonZeroI16;

/// One caller-owned offset alteration borrowed during protocol adaptation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OffsetCommitTargetRef<'a> {
    topic: &'a str,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<&'a str>,
}

impl<'a> OffsetCommitTargetRef<'a> {
    pub(crate) const fn new(
        topic: &'a str,
        partition: i32,
        next_offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<&'a str>,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch,
            metadata,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn next_offset(self) -> i64 {
        self.next_offset
    }

    pub(crate) const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }

    pub(crate) const fn metadata(self) -> Option<&'a str> {
        self.metadata
    }
}

/// Exact partition-level broker result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OffsetCommitPartitionResult {
    Altered,
    Rejected { code: NonZeroI16 },
}

/// One generated response fact correlated back to caller order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OffsetCommitPartitionRef<'a> {
    topic: &'a str,
    partition: i32,
    result: OffsetCommitPartitionResult,
    caller_index: usize,
}

impl<'a> OffsetCommitPartitionRef<'a> {
    pub(super) const fn new(
        topic: &'a str,
        partition: i32,
        result: OffsetCommitPartitionResult,
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

    pub(crate) const fn result(self) -> OffsetCommitPartitionResult {
        self.result
    }

    pub(super) const fn caller_index(self) -> usize {
        self.caller_index
    }
}

/// Borrowed, caller-ordered facts whose future owned allocation charge is proven.
#[must_use = "validated offset-alteration facts must be terminally interpreted"]
pub(crate) struct ValidatedOffsetCommitResponse<'a> {
    entries: Vec<OffsetCommitPartitionRef<'a>>,
    throttle_time_ms: u32,
    retained_charge: usize,
}

impl<'a> ValidatedOffsetCommitResponse<'a> {
    pub(super) const fn new(
        entries: Vec<OffsetCommitPartitionRef<'a>>,
        throttle_time_ms: u32,
        retained_charge: usize,
    ) -> Self {
        Self {
            entries,
            throttle_time_ms,
            retained_charge,
        }
    }

    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) const fn retained_charge(&self) -> usize {
        self.retained_charge
    }

    pub(crate) fn entries(&self) -> &[OffsetCommitPartitionRef<'a>] {
        &self.entries
    }

    pub(crate) fn into_validated_alterations(self) -> Vec<OffsetCommitPartitionRef<'a>> {
        self.entries
    }
}
