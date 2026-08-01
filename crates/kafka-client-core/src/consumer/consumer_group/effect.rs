//! Ordered mechanism instructions emitted by KIP-848 membership policy.

use crate::{AssignmentGeneration, Deadline, GroupId, LiveGroupAssignment, MemberId};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatFatal, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupHeartbeatRetrySchedule, ConsumerGroupHeartbeatSchedule, ConsumerGroupMemberEpoch,
};

/// One bounded mechanism action for a KIP-848 member.
#[derive(Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatEffect {
    /// Submit one generated `ConsumerGroupHeartbeat` request.
    Submit {
        /// Stable engine-catalog group identity.
        group_id: GroupId,
        /// Exact nonreused request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Join, steady, or leave request shape.
        kind: ConsumerGroupHeartbeatRequestKind,
        /// Stable member identity, absent only for the initial v0 join request.
        member_id: Option<MemberId>,
        /// Positive current member epoch, absent for an epoch-zero Join.
        member_epoch: Option<ConsumerGroupMemberEpoch>,
        /// Current owned assignment reported by steady and leave requests.
        assignment_generation: Option<AssignmentGeneration>,
        /// Original absolute attempt deadline.
        deadline: Deadline,
    },
    /// Rediscover the coordinator and replace the exact in-flight join or steady heartbeat once.
    Rediscover {
        /// Stable engine-catalog group identity used for coordinator discovery.
        group_id: GroupId,
        /// Exact original request identity retained by the replacement.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Join or steady request shape; leave replacement is deliberately unsupported.
        kind: ConsumerGroupHeartbeatRequestKind,
        /// Stable member identity, absent only for the initial v0 join request.
        member_id: Option<MemberId>,
        /// Positive current member epoch, absent for an epoch-zero Join.
        member_epoch: Option<ConsumerGroupMemberEpoch>,
        /// Current owned assignment reported by a steady replacement.
        assignment_generation: Option<AssignmentGeneration>,
        /// Original absolute attempt deadline, never restarted by rediscovery.
        deadline: Deadline,
    },
    /// Arm one positive core-owned delay before resubmitting the same loading-coordinator attempt.
    ArmCoordinatorLoadRetry {
        /// Exact attempt, request shape, backoff deadline, and original deadline fence.
        schedule: ConsumerGroupHeartbeatRetrySchedule,
    },
    /// Stage a broker target and arm cadence for the still-reportable assignment.
    Reconcile {
        /// Prior live assignment to retire before installation.
        previous: Option<LiveGroupAssignment>,
        /// New live assignment to install.
        assignment: LiveGroupAssignment,
        /// Exact broker member epoch paired with the new assignment.
        member_epoch: ConsumerGroupMemberEpoch,
        /// First heartbeat schedule fenced by the still-reportable assignment.
        schedule: ConsumerGroupHeartbeatSchedule,
    },
    /// Authorize installation of the exact retained target after its empty-owned acknowledgement.
    InstallReconciled {
        /// Stable member identity paired with the target.
        member_id: MemberId,
        /// Exact broker member epoch paired with the target.
        member_epoch: ConsumerGroupMemberEpoch,
        /// Exact target assignment generation retained by the interpreter.
        assignment_generation: AssignmentGeneration,
        /// First broker cadence fenced by the installed target.
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
