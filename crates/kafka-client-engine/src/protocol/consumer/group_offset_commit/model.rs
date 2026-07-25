//! Linear prepared commit snapshot and its exact retained-memory accounting.

use std::{mem::size_of, sync::Arc};

use kafka_client_core::{GroupOffsetCommitPartitionOutcome, OperationId, PartitionIndex, TopicId};

use crate::clock::OperationDeadline;

/// One request-order checkpoint entry retaining exact topic spelling.
#[derive(Debug)]
pub(super) struct PreparedGroupOffsetCommitEntry {
    pub(super) topic_id: TopicId,
    pub(super) topic: Arc<str>,
    pub(super) partition: PartitionIndex,
    pub(super) partition_index: i32,
    pub(super) next_offset: i64,
    pub(super) leader_epoch: Option<i32>,
}

impl PreparedGroupOffsetCommitEntry {
    pub(super) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub(super) fn topic(&self) -> &Arc<str> {
        &self.topic
    }

    pub(super) const fn partition(&self) -> PartitionIndex {
        self.partition
    }

    pub(super) const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    pub(super) const fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub(super) const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Concrete linear `OffsetCommit` attempt prepared before driver admission.
#[must_use = "a prepared group offset commit must be submitted or terminally settled"]
#[derive(Debug)]
pub(crate) struct PreparedGroupOffsetCommit {
    operation_id: OperationId,
    operation_deadline: OperationDeadline,
    group: Arc<str>,
    member: Arc<str>,
    classic_generation: i32,
    entries: Vec<PreparedGroupOffsetCommitEntry>,
    outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
    requires_leader_epoch: bool,
}

impl PreparedGroupOffsetCommit {
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact prepared ownership transfer"
    )]
    pub(super) fn new(
        operation_id: OperationId,
        operation_deadline: OperationDeadline,
        group: Arc<str>,
        member: Arc<str>,
        classic_generation: i32,
        entries: Vec<PreparedGroupOffsetCommitEntry>,
        outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
        requires_leader_epoch: bool,
    ) -> Self {
        Self {
            operation_id,
            operation_deadline,
            group,
            member,
            classic_generation,
            entries,
            outcomes,
            requires_leader_epoch,
        }
    }

    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) const fn operation_deadline(&self) -> OperationDeadline {
        self.operation_deadline
    }

    pub(crate) fn group(&self) -> &Arc<str> {
        &self.group
    }

    pub(super) fn member(&self) -> &Arc<str> {
        &self.member
    }

    pub(super) const fn classic_generation(&self) -> i32 {
        self.classic_generation
    }

    pub(super) fn entries(&self) -> &[PreparedGroupOffsetCommitEntry] {
        &self.entries
    }

    pub(crate) fn entries_capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub(crate) fn outcomes_capacity(&self) -> usize {
        self.outcomes.capacity()
    }

    #[cfg(test)]
    pub(super) fn entries_ptr_for_test(&self) -> *const PreparedGroupOffsetCommitEntry {
        self.entries.as_ptr()
    }

    #[cfg(test)]
    pub(super) fn outcomes_ptr_for_test(&self) -> *const GroupOffsetCommitPartitionOutcome {
        self.outcomes.as_ptr()
    }

    /// Returns the conservatively exact retained policy charge.
    ///
    /// The charge covers both vector capacities and each visible shared string
    /// span once; it does not claim allocator control-block or process-RSS
    /// accounting for shared `Arc` backing storage.
    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        let entry_bytes = self
            .entries
            .capacity()
            .checked_mul(size_of::<PreparedGroupOffsetCommitEntry>())?;
        let outcome_bytes = self
            .outcomes
            .capacity()
            .checked_mul(size_of::<GroupOffsetCommitPartitionOutcome>())?;
        let mut retained = self
            .group
            .len()
            .checked_add(self.member.len())?
            .checked_add(entry_bytes)?
            .checked_add(outcome_bytes)?;
        for (index, entry) in self.entries.iter().enumerate() {
            if !self.entries[..index]
                .iter()
                .any(|previous| previous.topic_id == entry.topic_id)
            {
                retained = retained.checked_add(entry.topic.len())?;
            }
        }
        Some(retained)
    }

    pub(super) fn take_outcomes(&mut self) -> Vec<GroupOffsetCommitPartitionOutcome> {
        core::mem::take(&mut self.outcomes)
    }

    pub(crate) const fn requires_leader_epoch(&self) -> bool {
        self.requires_leader_epoch
    }
}
