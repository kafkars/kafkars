//! Normalized time-stamped facts accepted by one classic membership owner.

use crate::{Deadline, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ClassicGeneration, ClassicJoinMembers, JoinedMemberSlot, MembershipCycle, TopicPartitionCount,
};

/// One explicit lifecycle fact with no protocol bytes or transport vocabulary.
#[derive(Debug, Eq, PartialEq)]
pub enum ClassicGroupInput {
    /// Starts one nonreused cycle at the original absolute deadline.
    Begin {
        /// Current monotonic observation supplied by the interpreter.
        now: Moment,
        /// Original absolute membership deadline.
        deadline: Deadline,
    },
    /// Join succeeded and this member is not the group leader.
    JoinFollower {
        /// Exact cycle that issued Join.
        cycle: MembershipCycle,
        /// Current monotonic observation.
        now: Moment,
        /// Normalized engine-catalog identity of this member.
        member_id: MemberId,
        /// Exact signed Kafka generation.
        generation: ClassicGeneration,
    },
    /// Join succeeded and this member owns the Range plan.
    JoinLeader {
        /// Exact cycle that issued Join.
        cycle: MembershipCycle,
        /// Current monotonic observation.
        now: Moment,
        /// Normalized engine-catalog identity of this member.
        member_id: MemberId,
        /// This member's correlation slot in the leader response.
        local_slot: JoinedMemberSlot,
        /// Exact signed Kafka generation.
        generation: ClassicGeneration,
        /// Bounded members ordered by Kafka member identity.
        members: ClassicJoinMembers,
    },
    /// Supplies exact topic partition counts for the leader's pending plan.
    PartitionCounts {
        /// Exact cycle awaiting partition counts.
        cycle: MembershipCycle,
        /// Current monotonic observation.
        now: Moment,
        /// Ordered scalar count facts.
        counts: Vec<TopicPartitionCount>,
    },
    /// Matching Sync succeeded with this member's decoded assignment.
    SyncSucceeded {
        /// Exact cycle that issued Sync.
        cycle: MembershipCycle,
        /// Current monotonic observation.
        now: Moment,
        /// Ordered unique assignment decoded by the engine.
        partitions: Vec<GroupAssignmentPartition>,
    },
    /// The exact Join attempt terminally failed without retry.
    JoinFailed {
        /// Exact cycle whose Join failed.
        cycle: MembershipCycle,
    },
    /// Partition-count acquisition terminally failed without retry.
    PartitionCountsFailed {
        /// Exact leader cycle whose count acquisition failed.
        cycle: MembershipCycle,
    },
    /// The exact Sync attempt terminally failed without retry.
    SyncFailed {
        /// Exact cycle whose Sync failed.
        cycle: MembershipCycle,
    },
    /// The stable assignment was explicitly lost before another cycle begins.
    AssignmentLost {
        /// Exact stable cycle whose assignment was lost.
        cycle: MembershipCycle,
    },
    /// The original absolute membership deadline elapsed.
    DeadlineElapsed {
        /// Exact expired cycle.
        cycle: MembershipCycle,
        /// Current monotonic observation proving expiration.
        now: Moment,
    },
    /// Permanently closes membership admission.
    Close,
}
