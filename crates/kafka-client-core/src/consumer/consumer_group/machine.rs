//! Unique deterministic owner of one KIP-848 member and assignment reconciliation.

use crate::{AssignmentGeneration, Deadline, GroupId, LiveGroupAssignment, MemberId};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatFatal, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatRetrySchedule,
    ConsumerGroupHeartbeatSchedule, ConsumerGroupHeartbeatSequence, ConsumerGroupMemberEpoch,
};

/// Deterministic KIP-848 member lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub struct ConsumerGroupHeartbeatMachine {
    pub(super) group_id: GroupId,
    pub(super) policy: ConsumerGroupHeartbeatPolicy,
    pub(super) phase: ConsumerGroupHeartbeatPhase,
    pub(super) next_sequence: Option<ConsumerGroupHeartbeatSequence>,
    pub(super) in_flight: Option<ConsumerGroupHeartbeatAttempt>,
    pub(super) deadline: Option<Deadline>,
    pub(super) rediscovery_replacement_used: bool,
    pub(super) retry_schedule: Option<ConsumerGroupHeartbeatRetrySchedule>,
    pub(super) member_id: Option<MemberId>,
    pub(super) member_epoch: Option<ConsumerGroupMemberEpoch>,
    pub(super) next_assignment_generation: Option<AssignmentGeneration>,
    pub(super) live_assignment: Option<LiveGroupAssignment>,
    pub(super) pending_assignment: Option<LiveGroupAssignment>,
    pub(super) schedule: Option<ConsumerGroupHeartbeatSchedule>,
    pub(super) fatal: Option<ConsumerGroupHeartbeatFatal>,
}

impl ConsumerGroupHeartbeatMachine {
    /// Creates one dormant owner without consulting time or emitting effects.
    pub const fn new(group_id: GroupId, policy: ConsumerGroupHeartbeatPolicy) -> Self {
        Self {
            group_id,
            policy,
            phase: ConsumerGroupHeartbeatPhase::Dormant,
            next_sequence: Some(ConsumerGroupHeartbeatSequence::initial()),
            in_flight: None,
            deadline: None,
            rediscovery_replacement_used: false,
            retry_schedule: None,
            member_id: None,
            member_epoch: None,
            next_assignment_generation: Some(AssignmentGeneration::initial()),
            live_assignment: None,
            pending_assignment: None,
            schedule: None,
            fatal: None,
        }
    }

    /// Returns the stable engine-catalog group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the current lifecycle phase.
    pub const fn phase(&self) -> ConsumerGroupHeartbeatPhase {
        self.phase
    }

    /// Returns the current exact in-flight heartbeat, when any.
    pub const fn in_flight(&self) -> Option<ConsumerGroupHeartbeatAttempt> {
        self.in_flight
    }

    /// Returns the current broker-issued member epoch.
    pub const fn member_epoch(&self) -> Option<ConsumerGroupMemberEpoch> {
        self.member_epoch
    }

    /// Borrows the current live assignment.
    pub const fn live_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.live_assignment.as_ref()
    }

    /// Borrows the broker target waiting for exact current-assignment retirement.
    pub const fn pending_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.pending_assignment.as_ref()
    }

    /// Returns the exact armed broker cadence.
    pub const fn schedule(&self) -> Option<ConsumerGroupHeartbeatSchedule> {
        self.schedule
    }

    /// Returns the exact armed coordinator-load retry schedule.
    pub const fn retry_schedule(&self) -> Option<ConsumerGroupHeartbeatRetrySchedule> {
        self.retry_schedule
    }

    /// Returns the retained terminal membership cause.
    pub const fn fatal(&self) -> Option<ConsumerGroupHeartbeatFatal> {
        self.fatal
    }
}
