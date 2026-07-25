//! Ordered mechanism instructions emitted by classic membership policy.

use crate::{Deadline, GroupId, LiveGroupAssignment, MemberId, TopicId};

use super::{
    ClassicAssignmentPlan, ClassicGeneration, ClassicGroupTiming, ClassicProtocol, MembershipCycle,
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
    },
    /// Revokes the prior live assignment before replacement or close.
    Revoke {
        /// Exact linear assignment being revoked.
        assignment: LiveGroupAssignment,
        /// Exact Kafka generation paired with the revoked assignment.
        classic_generation: ClassicGeneration,
    },
}

/// Zero or one action from one deterministic input.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicGroupTransition {
    effect: Option<ClassicGroupEffect>,
}

impl ClassicGroupTransition {
    pub(crate) const fn none() -> Self {
        Self { effect: None }
    }

    pub(crate) const fn one(effect: ClassicGroupEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Iterates over the optional mechanism action.
    pub fn effects(&self) -> impl Iterator<Item = &ClassicGroupEffect> {
        self.effect.iter()
    }

    /// Moves the optional mechanism action to the interpreter.
    pub fn into_effects(self) -> impl Iterator<Item = ClassicGroupEffect> {
        self.effect.into_iter()
    }
}
