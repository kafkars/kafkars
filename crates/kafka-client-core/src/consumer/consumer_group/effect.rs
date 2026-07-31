//! Ordered mechanism instructions emitted by KIP-848 membership policy.

use crate::{AssignmentGeneration, Deadline, GroupId, LiveGroupAssignment, MemberId};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatFatal, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatSchedule, ConsumerGroupMemberEpoch,
};

/// One bounded mechanism action for a KIP-848 member.
#[derive(Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatEffect {
    /// Submit one generated ConsumerGroupHeartbeat request.
    Submit {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Exact nonreused request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Join, steady, or leave request shape.
        kind: ConsumerGroupHeartbeatRequestKind,
        /// Stable member identity, absent only for the initial v0 join request.
        member_id: Option<MemberId>,
        /// Positive current member epoch, absent only for initial join.
        member_epoch: Option<ConsumerGroupMemberEpoch>,
        /// Current owned assignment reported by steady and leave requests.
        assignment_generation: Option<AssignmentGeneration>,
        /// Original absolute attempt deadline.
        deadline: Deadline,
    },
    /// Atomically replace any prior assignment and arm broker-controlled cadence.
    Reconcile {
        /// Prior live assignment to retire before installation.
        previous: Option<LiveGroupAssignment>,
        /// New live assignment to install.
        assignment: LiveGroupAssignment,
        /// Exact broker member epoch paired with the new assignment.
        member_epoch: ConsumerGroupMemberEpoch,
        /// First heartbeat schedule fenced by the new assignment.
        schedule: ConsumerGroupHeartbeatSchedule,
    },
    /// Arm one exact future steady heartbeat without changing assignment.
    ArmHeartbeat {
        /// Broker-paced assignment-fenced schedule.
        schedule: ConsumerGroupHeartbeatSchedule,
    },
    /// Retire one exact assignment during leave or terminal loss.
    Revoke {
        /// Exact linear live assignment being retired.
        assignment: LiveGroupAssignment,
    },
    /// Retain one exact terminal membership cause.
    Fatal {
        /// Attempt and stable normalized cause.
        fatal: ConsumerGroupHeartbeatFatal,
    },
}

/// Zero, one, or two ordered KIP-848 mechanism actions.
#[derive(Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatTransition {
    effects: [Option<ConsumerGroupHeartbeatEffect>; 2],
}

impl ConsumerGroupHeartbeatTransition {
    pub(crate) const fn none() -> Self {
        Self {
            effects: [None, None],
        }
    }

    pub(crate) const fn one(effect: ConsumerGroupHeartbeatEffect) -> Self {
        Self {
            effects: [Some(effect), None],
        }
    }

    pub(crate) const fn two(
        first: ConsumerGroupHeartbeatEffect,
        second: ConsumerGroupHeartbeatEffect,
    ) -> Self {
        Self {
            effects: [Some(first), Some(second)],
        }
    }

    /// Iterates over bounded ordered mechanism actions.
    pub fn effects(&self) -> impl Iterator<Item = &ConsumerGroupHeartbeatEffect> {
        self.effects.iter().flatten()
    }

    /// Moves bounded ordered mechanism actions to the interpreter.
    pub fn into_effects(self) -> impl Iterator<Item = ConsumerGroupHeartbeatEffect> {
        self.effects.into_iter().flatten()
    }
}
