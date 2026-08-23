//! Stable Rust vocabulary for current share-member assignment state.

/// One topic-partition currently assigned to a share member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareConsumerAssignmentPartition {
    topic: String,
    partition: i32,
}

impl ShareConsumerAssignmentPartition {
    pub(crate) const fn from_parts(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact Kafka topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// One broker-confirmed share membership and its ordered assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareConsumerAssignment {
    member_epoch: i32,
    assignment_epoch: u64,
    partitions: Box<[ShareConsumerAssignmentPartition]>,
}

impl ShareConsumerAssignment {
    pub(crate) fn from_parts(
        member_epoch: i32,
        assignment_epoch: u64,
        partitions: Vec<ShareConsumerAssignmentPartition>,
    ) -> Self {
        Self {
            member_epoch,
            assignment_epoch,
            partitions: partitions.into_boxed_slice(),
        }
    }

    /// Returns the positive broker-issued share-member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the nonreused local assignment fence.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }

    /// Borrows the ordered current topic-partition assignment.
    pub fn partitions(&self) -> &[ShareConsumerAssignmentPartition] {
        &self.partitions
    }
}
