//! Ordered mechanism instructions emitted by share membership policy.

use crate::{AssignmentGeneration, Deadline, GroupId, LiveGroupAssignment, MemberId};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatFatal, ShareGroupHeartbeatRequestKind,
    ShareGroupHeartbeatRetrySchedule, ShareGroupHeartbeatSchedule, ShareGroupMemberEpoch,
};

/// One bounded mechanism action for a share member.
#[derive(Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatEffect {
    /// Submit one generated `ShareGroupHeartbeat` request.
    Submit {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Stable consumer-generated member identity.
        member_id: MemberId,
        /// Exact nonreused request identity.
        attempt: ShareGroupHeartbeatAttempt,
        /// Join, steady, or leave request shape.
        kind: ShareGroupHeartbeatRequestKind,
        /// Current positive epoch, absent for an epoch-zero Join.
        member_epoch: Option<ShareGroupMemberEpoch>,
        /// Current local assignment fence, absent while awaiting assignment.
        assignment_generation: Option<AssignmentGeneration>,
        /// Original absolute attempt deadline.
        deadline: Deadline,
    },
    /// Invalidate one stale share-coordinator route.
    Rediscover {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Stable consumer-generated member identity.
        member_id: MemberId,
        /// Fresh nonreused replacement request.
        attempt: ShareGroupHeartbeatAttempt,
        /// Retained request shape.
        kind: ShareGroupHeartbeatRequestKind,
        /// Current positive epoch, absent for Join.
        member_epoch: Option<ShareGroupMemberEpoch>,
        /// Current local assignment fence.
        assignment_generation: Option<AssignmentGeneration>,
        /// Original absolute attempt deadline.
        deadline: Deadline,
    },
    /// Arm one exact positive retry delay.
    ArmRetry {
        /// Exact attempt, cause, delay, and original-deadline fence.
        schedule: ShareGroupHeartbeatRetrySchedule,
    },
    /// Replace the observable assignment under one new local generation.
    ReplaceAssignment {
        /// Prior assignment to retire, when one existed.
        previous: Option<LiveGroupAssignment>,
        /// New assignment to install.
        assignment: LiveGroupAssignment,
        /// Exact accepted broker member epoch.
        member_epoch: ShareGroupMemberEpoch,
        /// First cadence fenced by the installed assignment.
        schedule: ShareGroupHeartbeatSchedule,
    },
    /// Retain accepted membership while Kafka computes an assignment.
    AwaitAssignment {
        /// Exact accepted broker member epoch.
        member_epoch: ShareGroupMemberEpoch,
        /// Broker-paced assignment-less cadence.
        schedule: ShareGroupHeartbeatSchedule,
    },
    /// Arm a future heartbeat without changing assignment.
    ArmHeartbeat {
        /// Broker-paced assignment-fenced schedule.
        schedule: ShareGroupHeartbeatSchedule,
    },
    /// Retire one exact assignment during recovery, leave, or loss.
    Revoke {
        /// Exact linear assignment being retired.
        assignment: LiveGroupAssignment,
    },
    /// Retain one exact terminal membership cause.
    Fatal {
        /// Attempt and stable normalized cause.
        fatal: ShareGroupHeartbeatFatal,
    },
}

/// Zero, one, or two ordered share-membership actions.
#[derive(Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatTransition {
    effects: [Option<ShareGroupHeartbeatEffect>; 2],
}

impl ShareGroupHeartbeatTransition {
    pub(super) const fn none() -> Self {
        Self {
            effects: [None, None],
        }
    }

    pub(super) const fn one(effect: ShareGroupHeartbeatEffect) -> Self {
        Self {
            effects: [Some(effect), None],
        }
    }

    pub(super) const fn two(
        first: ShareGroupHeartbeatEffect,
        second: ShareGroupHeartbeatEffect,
    ) -> Self {
        Self {
            effects: [Some(first), Some(second)],
        }
    }

    /// Iterates over bounded ordered actions.
    pub fn effects(&self) -> impl Iterator<Item = &ShareGroupHeartbeatEffect> {
        self.effects.iter().flatten()
    }

    /// Moves bounded ordered actions to the interpreter.
    pub fn into_effects(self) -> impl Iterator<Item = ShareGroupHeartbeatEffect> {
        self.effects.into_iter().flatten()
    }
}
