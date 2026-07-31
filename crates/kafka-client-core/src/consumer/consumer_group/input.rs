//! Normalized facts accepted by one KIP-848 membership owner.

use crate::{Deadline, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatSchedule,
    ConsumerGroupMemberEpoch,
};

/// One explicit KIP-848 lifecycle fact without protocol or transport vocabulary.
#[derive(Debug, Eq, PartialEq)]
pub enum ConsumerGroupHeartbeatInput {
    /// Starts the initial epoch-zero heartbeat under the original public deadline.
    Begin {
        /// Current monotonic observation supplied by the interpreter.
        now: Moment,
        /// Original absolute membership-start deadline.
        deadline: Deadline,
    },
    /// Observes one exact broker-controlled cadence deadline.
    HeartbeatDue {
        /// Full schedule returned by the interpreter.
        schedule: ConsumerGroupHeartbeatSchedule,
        /// Current monotonic observation proving cadence expiry.
        now: Moment,
    },
    /// Applies one successful join or steady heartbeat response.
    HeartbeatSucceeded {
        /// Exact in-flight request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Current monotonic response observation.
        now: Moment,
        /// Stable catalog identity for the exact Kafka member spelling.
        member_id: MemberId,
        /// Positive coordinator-issued member epoch.
        member_epoch: ConsumerGroupMemberEpoch,
        /// Positive broker-controlled heartbeat interval in deterministic ticks.
        heartbeat_interval_ticks: u64,
        /// Nonnegative quota delay in deterministic ticks.
        throttle_ticks: u64,
        /// Ordered assignment when changed; absence means retain the live assignment.
        assignment: Option<Vec<GroupAssignmentPartition>>,
    },
    /// Applies one exact terminal join or steady heartbeat failure.
    HeartbeatFailed {
        /// Exact in-flight request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Stable normalized failure category.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Begins an epoch-minus-one leave heartbeat under an explicit close deadline.
    BeginLeave {
        /// Current monotonic observation supplied by the interpreter.
        now: Moment,
        /// Original absolute close deadline.
        deadline: Deadline,
    },
    /// Confirms that one exact leave heartbeat reached a successful terminal response.
    LeaveSucceeded {
        /// Exact in-flight leave request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
    },
    /// Applies one exact terminal leave heartbeat failure.
    LeaveFailed {
        /// Exact in-flight leave request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Stable normalized failure category.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Closes local ownership after no driver operation can still complete.
    Close,
}
