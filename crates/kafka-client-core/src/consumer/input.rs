//! Facts and direct control requests accepted by the assigned-consumer owner.
//!
//! `Assign`, `AddAssignments`, `Resume`, and `Seek` carry an absolute
//! child-resolution deadline captured before the public call enters the engine.
//! Core never reconstructs that deadline from a later relative timeout.

use super::{
    AssignedConsumerCloseId, AssignedPartition, AssignedTopicPartition, AssignmentEpoch,
    FetchFailure, FetchFence, FetchRecords, NextFetchOffset, PositionFence,
    PositionResolutionAttemptFailure, StartPosition,
};
use crate::{Deadline, Moment};

/// One deterministic direct-assignment transition input.
#[derive(Debug, Eq, PartialEq)]
pub enum AssignedConsumerInput {
    /// Accepts the sole close after its terminal capacity has been reserved.
    ///
    /// The engine must reserve one terminal completion before applying this
    /// input and release that reservation if core rejects the input.
    BeginClose,
    /// Reports that every cleanup effect and accepted driver call drained.
    CloseDrained {
        /// Exact core-owned close whose mechanisms finished draining.
        close_id: AssignedConsumerCloseId,
    },
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
    /// Adds new partitions without disturbing retained partition positions.
    AddAssignments {
        /// Caller-ordered partitions and their initial positions.
        partitions: Vec<AssignedPartition>,
        /// Monotonic observation captured at this public operation boundary.
        now: Moment,
        /// Absolute child-resolution deadline captured before this input reached the core.
        ///
        /// Explicit offsets need no resolution and therefore ignore this value.
        resolution_deadline: Deadline,
    },
    /// Removes assigned partitions without disturbing surviving partition positions.
    RemoveAssignments {
        /// Caller-ordered topic-partitions to remove.
        partitions: Vec<AssignedTopicPartition>,
    },
    /// Retires the exact optional assignment observed by classic-group ownership.
    RetireAssignment {
        /// Exact complete-assignment control revision, or `None` when unassigned.
        assignment_epoch: Option<AssignmentEpoch>,
    },
    /// Fences and pauses one assigned partition.
    Pause {
        /// Current complete-assignment control revision observed by the caller.
        assignment_epoch: AssignmentEpoch,
        /// Partition to pause.
        partition: AssignedTopicPartition,
    },
    /// Resumes one paused partition at its retained next position.
    Resume {
        /// Current complete-assignment control revision observed by the caller.
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
        /// Current complete-assignment control revision observed by the caller.
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
        /// Exact semantic terminal fact observed by the engine.
        failure: PositionResolutionAttemptFailure,
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
        /// Whether the engine retains application-visible records for this result.
        records: FetchRecords,
        /// Next offset after the normalized fetch response.
        next_offset: NextFetchOffset,
        /// Monotonic observation when the successful fetch was applied.
        now: Moment,
        /// Exact nonnegative broker throttle duration in deterministic clock ticks.
        throttle_ticks: u64,
    },
    /// Reports terminal failure of one exact fetch execution.
    FetchFailed {
        /// Exact fetch execution being settled.
        fence: FetchFence,
        /// Normalized semantic terminal reason, free of driver and wire types.
        failure: FetchFailure,
    },
    /// Authorizes one replacement for an exact recoverable Fetch attempt.
    FetchRetryAuthorized {
        /// Exact completed attempt whose position and offset must be preserved.
        fence: FetchFence,
    },
    /// Reports that one exact successful-Fetch throttle elapsed.
    FetchThrottleElapsed {
        /// Exact future fetch fenced by the timer.
        fence: FetchFence,
        /// Monotonic observation proving the throttle deadline elapsed.
        now: Moment,
    },
}
