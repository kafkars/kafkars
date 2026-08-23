//! Unique deterministic owner of one share member and current assignment.

use crate::{AssignmentGeneration, Deadline, GroupId, LiveGroupAssignment, MemberId};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatFatal, ShareGroupHeartbeatPhase,
    ShareGroupHeartbeatPolicy, ShareGroupHeartbeatRetrySchedule, ShareGroupHeartbeatSchedule,
    ShareGroupHeartbeatSequence, ShareGroupMemberEpoch,
};

/// Deterministic KIP-932 share-member lifecycle.
#[derive(Debug, Eq, PartialEq)]
pub struct ShareGroupHeartbeatMachine {
    pub(super) group_id: GroupId,
    pub(super) member_id: MemberId,
    pub(super) policy: ShareGroupHeartbeatPolicy,
    pub(super) phase: ShareGroupHeartbeatPhase,
    pub(super) next_sequence: Option<ShareGroupHeartbeatSequence>,
    pub(super) in_flight: Option<ShareGroupHeartbeatAttempt>,
    pub(super) deadline: Option<Deadline>,
    pub(super) retry_schedule: Option<ShareGroupHeartbeatRetrySchedule>,
    pub(super) member_epoch: Option<ShareGroupMemberEpoch>,
    pub(super) next_assignment_generation: Option<AssignmentGeneration>,
    pub(super) live_assignment: Option<LiveGroupAssignment>,
    pub(super) schedule: Option<ShareGroupHeartbeatSchedule>,
    pub(super) fatal: Option<ShareGroupHeartbeatFatal>,
    pub(super) initial_heartbeat_succeeded: bool,
}

impl ShareGroupHeartbeatMachine {
    /// Creates one dormant owner without consulting time or emitting effects.
    pub const fn new(
        group_id: GroupId,
        member_id: MemberId,
        policy: ShareGroupHeartbeatPolicy,
    ) -> Self {
        Self {
            group_id,
            member_id,
            policy,
            phase: ShareGroupHeartbeatPhase::Dormant,
            next_sequence: Some(ShareGroupHeartbeatSequence::initial()),
            in_flight: None,
            deadline: None,
            retry_schedule: None,
            member_epoch: None,
            next_assignment_generation: Some(AssignmentGeneration::initial()),
            live_assignment: None,
            schedule: None,
            fatal: None,
            initial_heartbeat_succeeded: false,
        }
    }

    /// Returns the stable engine-catalog group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the stable consumer-generated member identity.
    pub const fn member_id(&self) -> MemberId {
        self.member_id
    }

    /// Returns the current lifecycle phase.
    pub const fn phase(&self) -> ShareGroupHeartbeatPhase {
        self.phase
    }

    /// Returns the current exact in-flight heartbeat.
    pub const fn in_flight(&self) -> Option<ShareGroupHeartbeatAttempt> {
        self.in_flight
    }

    /// Returns the current broker-issued member epoch.
    pub const fn member_epoch(&self) -> Option<ShareGroupMemberEpoch> {
        self.member_epoch
    }

    /// Borrows the current assignment.
    pub const fn live_assignment(&self) -> Option<&LiveGroupAssignment> {
        self.live_assignment.as_ref()
    }

    /// Returns the exact broker cadence.
    pub const fn schedule(&self) -> Option<ShareGroupHeartbeatSchedule> {
        self.schedule
    }

    /// Returns the exact retained retry schedule.
    pub const fn retry_schedule(&self) -> Option<ShareGroupHeartbeatRetrySchedule> {
        self.retry_schedule
    }

    /// Returns the retained terminal membership cause.
    pub const fn fatal(&self) -> Option<ShareGroupHeartbeatFatal> {
        self.fatal
    }

    /// Returns a retained terminal only before the first accepted heartbeat.
    pub const fn startup_fatal(&self) -> Option<ShareGroupHeartbeatFatal> {
        if self.initial_heartbeat_succeeded {
            None
        } else {
            self.fatal
        }
    }
}
