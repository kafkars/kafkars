//! Linear ownership of one core-requested classic-leader partition-count read.

use kafka_client_core::{MembershipCycle, TopicId, TopicPartitionCount};

use crate::clock::OperationDeadline;

/// Exact ordered scalar facts requested by core before it can build Range.
#[must_use = "partition-count ownership must become facts, expire, or close"]
#[expect(
    clippy::struct_field_names,
    reason = "qualified field names keep every partition-count fence explicit"
)]
pub(super) struct PreparedClassicGroupPartitionCounts {
    partition_count_cycle: MembershipCycle,
    partition_count_topics: Vec<TopicId>,
    partition_count_values: Vec<TopicPartitionCount>,
    partition_count_metadata_generation: Option<u64>,
    partition_count_deadline: OperationDeadline,
}

impl PreparedClassicGroupPartitionCounts {
    pub(super) fn try_new(
        cycle: MembershipCycle,
        topics: Vec<TopicId>,
        deadline: OperationDeadline,
    ) -> Result<Self, ClassicGroupPartitionCountProgressError> {
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(topics.len())
            .map_err(|_error| ClassicGroupPartitionCountProgressError::Allocation)?;
        Ok(Self {
            partition_count_cycle: cycle,
            partition_count_topics: topics,
            partition_count_values: counts,
            partition_count_metadata_generation: None,
            partition_count_deadline: deadline,
        })
    }

    pub(super) const fn cycle(&self) -> MembershipCycle {
        self.partition_count_cycle
    }

    pub(super) fn topics(&self) -> &[TopicId] {
        &self.partition_count_topics
    }

    pub(super) fn next_topic(&self) -> Option<TopicId> {
        self.partition_count_topics
            .get(self.partition_count_values.len())
            .copied()
    }

    pub(super) fn append(
        &mut self,
        topic_id: TopicId,
        count: u32,
        metadata_generation: u64,
    ) -> Result<ClassicGroupPartitionCountProgress, ClassicGroupPartitionCountProgressError> {
        if self.next_topic() != Some(topic_id) {
            return Err(ClassicGroupPartitionCountProgressError::TopicFence);
        }
        if !self.partition_count_values.is_empty()
            && self
                .partition_count_metadata_generation
                .is_some_and(|generation| generation != metadata_generation)
        {
            self.partition_count_values.clear();
            self.partition_count_metadata_generation = Some(metadata_generation);
            return Ok(ClassicGroupPartitionCountProgress::Restarted);
        }
        self.partition_count_metadata_generation = Some(metadata_generation);
        self.partition_count_values
            .push(TopicPartitionCount::new(topic_id, count));
        Ok(ClassicGroupPartitionCountProgress::Advanced)
    }

    pub(super) fn is_complete(&self) -> bool {
        self.partition_count_values.len() == self.partition_count_topics.len()
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> &[TopicPartitionCount] {
        &self.partition_count_values
    }

    pub(super) fn try_clone_completed_counts(
        &self,
    ) -> Result<Vec<TopicPartitionCount>, ClassicGroupPartitionCountProgressError> {
        if !self.is_complete() {
            return Err(ClassicGroupPartitionCountProgressError::Incomplete);
        }
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(self.partition_count_values.len())
            .map_err(|_error| ClassicGroupPartitionCountProgressError::Allocation)?;
        counts.extend_from_slice(&self.partition_count_values);
        Ok(counts)
    }

    pub(super) const fn deadline(&self) -> OperationDeadline {
        self.partition_count_deadline
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountProgress {
    Advanced,
    Restarted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountProgressError {
    Allocation,
    Incomplete,
    TopicFence,
}
