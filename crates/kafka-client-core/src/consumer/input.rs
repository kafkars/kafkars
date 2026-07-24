//! Facts and direct control requests accepted by the assigned-consumer owner.

use super::{
    AssignedPartition, AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset,
    PositionFence, StartPosition,
};

/// One deterministic direct-assignment transition input.
#[derive(Debug, Eq, PartialEq)]
pub enum AssignedConsumerInput {
    /// Replaces the complete direct assignment in caller order.
    Assign {
        /// Explicit topic-partition start positions.
        partitions: Vec<AssignedPartition>,
    },
    /// Fences and pauses one assigned partition.
    Pause {
        /// Assignment generation observed by the caller.
        assignment_epoch: AssignmentEpoch,
        /// Partition to pause.
        partition: AssignedTopicPartition,
    },
    /// Resumes one paused partition at its retained next position.
    Resume {
        /// Assignment generation observed by the caller.
        assignment_epoch: AssignmentEpoch,
        /// Partition to resume.
        partition: AssignedTopicPartition,
    },
    /// Fences outstanding work and replaces one partition position.
    Seek {
        /// Assignment generation observed by the caller.
        assignment_epoch: AssignmentEpoch,
        /// Partition whose position changes.
        partition: AssignedTopicPartition,
        /// Replacement start position.
        position: StartPosition,
    },
    /// Reports a Kafka-resolved beginning or end position.
    PositionResolved {
        /// Exact position request being settled.
        fence: PositionFence,
        /// Resolved next-fetch offset.
        next_offset: NextFetchOffset,
    },
    /// Advances one exact completed fetch to its next position.
    FetchAdvanced {
        /// Exact fetch execution being settled.
        fence: FetchFence,
        /// Next offset after the normalized fetch response.
        next_offset: NextFetchOffset,
    },
}
