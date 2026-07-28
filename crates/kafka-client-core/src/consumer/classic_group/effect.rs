//! Ordered mechanism instructions emitted by classic membership policy.

use crate::{Deadline, GroupId, LiveGroupAssignment, MemberId, TopicId};

use super::{
    ClassicAssignmentPlan, ClassicCoordinatorRecovery, ClassicGeneration, ClassicGroupFatal,
    ClassicGroupTiming, ClassicHeartbeatAttempt, ClassicHeartbeatSchedule, ClassicProtocol,
    ClassicRejoinSchedule, MembershipCycle,
};

/// One bounded mechanism action carrying the original membership deadline.
#[derive(Debug, Eq, PartialEq)]
pub enum ClassicGroupEffect {
    /// Submit `JoinGroup` for one exact cycle.
    Join {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Nonreused membership cycle.
        cycle: MembershipCycle,
        /// Core-selected classic assignment protocol.
        protocol: ClassicProtocol,
        /// Assigned identity for a KIP-394 replacement, absent on initial Join.
        member_id: Option<MemberId>,
        /// Immutable positive wire-representable Join timeout policy.
        timing: ClassicGroupTiming,
        /// Original absolute deadline.
        deadline: Deadline,
    },
    /// Resolve exact partition counts before leader-side Range planning.
    RequestPartitionCounts {
        /// Nonreused membership cycle.
        cycle: MembershipCycle,
        /// Ordered unique engine-catalog topics.
        topics: Vec<TopicId>,
        /// Original absolute deadline.
        deadline: Deadline,
    },
    /// Submit `SyncGroup` with either an empty follower plan or full leader plan.
    Sync {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Nonreused membership cycle.
        cycle: MembershipCycle,
        /// Joined local member identity.
        member_id: MemberId,
        /// Exact signed Kafka generation.
        generation: ClassicGeneration,
        /// Complete slot-keyed assignment plan.
        plan: ClassicAssignmentPlan,
        /// Original absolute deadline.
        deadline: Deadline,
    },
    /// Installs one separately owned live assignment after matching Sync success.
    Install {
        /// Linear assignment copy for the effect interpreter.
        assignment: LiveGroupAssignment,
        /// Exact Kafka generation paired with the installed assignment.
        classic_generation: ClassicGeneration,
        /// First exact heartbeat schedule owned by the installed assignment.
        heartbeat: ClassicHeartbeatSchedule,
    },
    /// Arm one exact future heartbeat cadence deadline.
    ArmHeartbeat {
        /// Assignment-fenced heartbeat identity and due deadline.
        schedule: ClassicHeartbeatSchedule,
    },
    /// Submit one exact heartbeat with a separately owned attempt deadline.
    SubmitHeartbeat {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Exact assignment and sequence fence.
        attempt: ClassicHeartbeatAttempt,
        /// Joined local member identity.
        member_id: MemberId,
        /// Exact signed Kafka generation.
        classic_generation: ClassicGeneration,
        /// Absolute deadline for this heartbeat attempt.
        deadline: Deadline,
    },
    /// Revokes the prior live assignment before replacement or close.
    Revoke {
        /// Exact linear assignment being revoked.
        assignment: LiveGroupAssignment,
        /// Exact Kafka generation paired with the revoked assignment.
        classic_generation: ClassicGeneration,
    },
    /// Arms one exact recovery deadline and reports coordinator ownership needs.
    ArmRejoin {
        /// Cycle or assignment-fenced recovery schedule.
        schedule: ClassicRejoinSchedule,
        /// Opaque coordinator recovery need for the interpreter.
        coordinator: ClassicCoordinatorRecovery,
    },
    /// Reports that this machine retained a terminal membership cause.
    Fatal {
        /// Exact cycle-fenced terminal state.
        fatal: ClassicGroupFatal,
    },
}

/// Zero, one, or two ordered actions from one deterministic input.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicGroupTransition {
    effects: [Option<ClassicGroupEffect>; 2],
}

impl ClassicGroupTransition {
    pub(crate) const fn none() -> Self {
        Self {
            effects: [None, None],
        }
    }

    pub(crate) const fn one(effect: ClassicGroupEffect) -> Self {
        Self {
            effects: [Some(effect), None],
        }
    }

    pub(crate) const fn two(first: ClassicGroupEffect, second: ClassicGroupEffect) -> Self {
        Self {
            effects: [Some(first), Some(second)],
        }
    }

    /// Iterates over the bounded ordered mechanism actions.
    pub fn effects(&self) -> impl Iterator<Item = &ClassicGroupEffect> {
        self.effects.iter().flatten()
    }

    /// Moves the bounded ordered mechanism actions to the interpreter.
    pub fn into_effects(self) -> impl Iterator<Item = ClassicGroupEffect> {
        self.effects.into_iter().flatten()
    }
}
