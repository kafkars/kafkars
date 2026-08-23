//! Complete broker-session and attempt fences for `ShareFetch`.

use crate::{Deadline, GroupId, MemberId, ShareGroupMemberEpoch};

use super::{
    ShareConnectionGeneration, ShareFetchAssignmentGeneration, ShareFetchBrokerId,
    ShareRouteGeneration,
};

/// Nonnegative `ShareFetch` session epoch, with zero opening a new session.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareFetchSessionEpoch(i32);

impl ShareFetchSessionEpoch {
    /// Returns Kafka's initial-session epoch.
    pub const fn initial() -> Self {
        Self(0)
    }

    /// Restores one nonnegative live-session epoch.
    pub const fn try_from_raw(value: i32) -> Option<Self> {
        if value < 0 { None } else { Some(Self(value)) }
    }

    /// Returns the exact Kafka request epoch.
    pub const fn get(self) -> i32 {
        self.0
    }

    const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Complete authority fence for one broker-local `ShareFetch` session.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ShareFetchSessionFence {
    broker_id: ShareFetchBrokerId,
    route_generation: ShareRouteGeneration,
    connection_generation: ShareConnectionGeneration,
    group_id: GroupId,
    member_id: MemberId,
    member_epoch: ShareGroupMemberEpoch,
    session_epoch: ShareFetchSessionEpoch,
}

impl ShareFetchSessionFence {
    /// Joins independently owned routing, connection, membership, and session facts.
    pub const fn new(
        broker_id: ShareFetchBrokerId,
        route_generation: ShareRouteGeneration,
        connection_generation: ShareConnectionGeneration,
        group_id: GroupId,
        member_id: MemberId,
        member_epoch: ShareGroupMemberEpoch,
        session_epoch: ShareFetchSessionEpoch,
    ) -> Self {
        Self {
            broker_id,
            route_generation,
            connection_generation,
            group_id,
            member_id,
            member_epoch,
            session_epoch,
        }
    }

    /// Returns the broker owning this session.
    pub const fn broker_id(self) -> ShareFetchBrokerId {
        self.broker_id
    }

    /// Returns the exact driver route generation.
    pub const fn route_generation(self) -> ShareRouteGeneration {
        self.route_generation
    }

    /// Returns the exact connection generation.
    pub const fn connection_generation(self) -> ShareConnectionGeneration {
        self.connection_generation
    }

    /// Returns the stable group identity.
    pub const fn group_id(self) -> GroupId {
        self.group_id
    }

    /// Returns the stable member identity.
    pub const fn member_id(self) -> MemberId {
        self.member_id
    }

    /// Returns the current broker-issued member epoch.
    pub const fn member_epoch(self) -> ShareGroupMemberEpoch {
        self.member_epoch
    }

    /// Returns the current `ShareFetch` session epoch.
    pub const fn session_epoch(self) -> ShareFetchSessionEpoch {
        self.session_epoch
    }

    pub(in crate::consumer::share_fetch) const fn next_session(self) -> Option<Self> {
        match self.session_epoch.checked_next() {
            Some(session_epoch) => Some(Self {
                session_epoch,
                ..self
            }),
            None => None,
        }
    }
}

/// Exact identity and original deadline of one nonoverlapping `ShareFetch` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareFetchAttempt {
    fence: ShareFetchSessionFence,
    assignment_generation: ShareFetchAssignmentGeneration,
    deadline: Deadline,
}

impl ShareFetchAttempt {
    /// Captures one session, assignment, and public or background deadline.
    pub const fn new(
        fence: ShareFetchSessionFence,
        assignment_generation: ShareFetchAssignmentGeneration,
        deadline: Deadline,
    ) -> Self {
        Self {
            fence,
            assignment_generation,
            deadline,
        }
    }

    /// Returns the complete broker-session fence.
    pub const fn fence(self) -> ShareFetchSessionFence {
        self.fence
    }

    /// Returns the exact assignment snapshot.
    pub const fn assignment_generation(self) -> ShareFetchAssignmentGeneration {
        self.assignment_generation
    }

    /// Returns the unchanged absolute attempt deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }
}
