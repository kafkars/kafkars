//! Normalized time-stamped facts accepted by one classic membership owner.

use crate::{Deadline, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ClassicBrokerError, ClassicGeneration, ClassicHeartbeatAttempt, ClassicJoinMembers,
    ClassicRejoinSchedule, JoinedMemberSlot, MembershipCycle, TopicPartitionCount,
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
    /// The broker assigned the identity required for a same-cycle KIP-394 Join replacement.
    JoinMemberIdRequired {
        /// Exact cycle that issued the rejected Join.
        cycle: MembershipCycle,
        /// Current monotonic observation.
        now: Moment,
        /// Broker-assigned identity, absent when the response was malformed.
        assigned_member_id: Option<MemberId>,
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
    /// The exact assignment-fenced heartbeat cadence deadline elapsed.
    HeartbeatDue {
        /// Exact heartbeat identity awaiting submission.
        attempt: ClassicHeartbeatAttempt,
        /// Current monotonic observation proving cadence expiry.
        now: Moment,
    },
    /// The exact heartbeat succeeded before its attempt deadline.
    HeartbeatSucceeded {
        /// Exact in-flight heartbeat identity.
        attempt: ClassicHeartbeatAttempt,
        /// Current monotonic response observation.
        now: Moment,
        /// Nonnegative broker throttle converted to deterministic ticks.
        throttle_ticks: u64,
    },
    /// The exact heartbeat received one nonzero Kafka broker rejection.
    HeartbeatRejected {
        /// Exact in-flight heartbeat identity.
        attempt: ClassicHeartbeatAttempt,
        /// Current monotonic response observation.
        now: Moment,
        /// Exact nonzero Kafka error code.
        error: ClassicBrokerError,
    },
    /// The exact heartbeat lost its coordinator route before a broker response.
    HeartbeatCoordinatorLost {
        /// Exact in-flight heartbeat identity.
        attempt: ClassicHeartbeatAttempt,
        /// Current monotonic failure observation.
        now: Moment,
    },
    /// The exact heartbeat terminally failed without retry.
    HeartbeatFailed {
        /// Exact in-flight heartbeat identity.
        attempt: ClassicHeartbeatAttempt,
    },
    /// The exact heartbeat attempt deadline elapsed.
    HeartbeatDeadlineElapsed {
        /// Exact in-flight heartbeat identity.
        attempt: ClassicHeartbeatAttempt,
        /// Current monotonic observation proving expiration.
        now: Moment,
    },
    /// The exact Join attempt terminally failed without retry.
    JoinFailed {
        /// Exact cycle whose Join failed.
        cycle: MembershipCycle,
    },
    /// The exact Join received one nonzero Kafka broker rejection.
    JoinRejected {
        /// Exact cycle whose Join was rejected.
        cycle: MembershipCycle,
        /// Current monotonic response observation.
        now: Moment,
        /// Exact nonzero Kafka error code.
        error: ClassicBrokerError,
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
    /// The exact Sync received one nonzero Kafka broker rejection.
    SyncRejected {
        /// Exact cycle whose Sync was rejected.
        cycle: MembershipCycle,
        /// Current monotonic response observation.
        now: Moment,
        /// Exact nonzero Kafka error code.
        error: ClassicBrokerError,
    },
    /// One exact pending recovery schedule reached its due deadline.
    RejoinDue {
        /// Full cycle or assignment-fenced schedule returned by the interpreter.
        schedule: ClassicRejoinSchedule,
        /// Current monotonic observation proving recovery is due.
        now: Moment,
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
