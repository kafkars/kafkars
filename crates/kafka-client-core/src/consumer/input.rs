//! Facts and direct control requests accepted by the assigned-consumer owner.
//!
//! `Assign`, `Resume`, and `Seek` carry an absolute child-resolution deadline
//! captured before the public call enters the engine. Core never reconstructs
//! that deadline from a later relative timeout.

use super::{
    AssignedPartition, AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset,
    PositionFence, StartPosition,
};
use crate::{Deadline, Moment};

/// One deterministic direct-assignment transition input.
#[derive(Debug, Eq, PartialEq)]
pub enum AssignedConsumerInput {
    /// Replaces the complete direct assignment in caller order.
    Assign {
        /// Explicit topic-partition start positions.
        partitions: Vec<AssignedPartition>,
        /// Monotonic observation captured at this public operation boundary.
        now: Moment,
        /// Absolute child-resolution deadline captured before this input reached the core.
        ///
        /// Explicit offsets need no resolution and therefore ignore this value.
        resolution_deadline: Deadline,
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
        /// Monotonic observation captured at this public operation boundary.
        now: Moment,
        /// Absolute child-resolution deadline captured before this input reached the core.
        ///
        /// Ready offsets and retained throttles ignore this value.
        resolution_deadline: Deadline,
    },
    /// Fences outstanding work and replaces one partition position.
    Seek {
        /// Assignment generation observed by the caller.
        assignment_epoch: AssignmentEpoch,
        /// Partition whose position changes.
        partition: AssignedTopicPartition,
        /// Replacement start position.
        position: StartPosition,
        /// Monotonic observation captured at this public operation boundary.
        now: Moment,
        /// Absolute child-resolution deadline captured before this input reached the core.
        ///
        /// Explicit offsets need no resolution and therefore ignore this value.
        resolution_deadline: Deadline,
    },
    /// Reports a Kafka-resolved beginning or end position.
    PositionResolved {
        /// Exact position request being settled.
        fence: PositionFence,
        /// Resolved next-fetch offset.
        next_offset: NextFetchOffset,
        /// Monotonic observation when the terminal result was applied.
        now: Moment,
        /// Positive broker throttle duration in deterministic clock ticks.
        throttle_ticks: u64,
    },
    /// Reports terminal failure of one exact position resolution.
    PositionResolutionFailed {
        /// Exact position request being settled.
        fence: PositionFence,
        /// Monotonic observation used for deadline precedence.
        now: Moment,
    },
    /// Reports that one exact position-resolution deadline elapsed.
    PositionResolutionDeadlineElapsed {
        /// Exact position request being expired.
        fence: PositionFence,
        /// Monotonic observation proving the deadline elapsed.
        now: Moment,
    },
    /// Reports that one exact positive broker throttle elapsed.
    PositionThrottleElapsed {
        /// Exact throttled position being released.
        fence: PositionFence,
        /// Monotonic observation proving the throttle deadline elapsed.
        now: Moment,
    },
    /// Advances one exact completed fetch to its next position.
    FetchAdvanced {
        /// Exact fetch execution being settled.
        fence: FetchFence,
        /// Next offset after the normalized fetch response.
        next_offset: NextFetchOffset,
    },
}
