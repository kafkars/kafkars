//! Normalized facts accepted by one share-membership owner.

use crate::{Deadline, GroupAssignmentPartition, Moment};

use super::{
    ShareGroupHeartbeatAttempt, ShareGroupHeartbeatFailure, ShareGroupHeartbeatRetrySchedule,
    ShareGroupHeartbeatSchedule, ShareGroupMemberEpoch,
};

/// One explicit share-group lifecycle fact without protocol vocabulary.
#[derive(Debug, Eq, PartialEq)]
pub enum ShareGroupHeartbeatInput {
    /// Starts the initial epoch-zero heartbeat under the public deadline.
    Begin {
        /// Current monotonic observation.
        now: Moment,
        /// Original absolute membership-start deadline.
        deadline: Deadline,
    },
    /// Observes one exact broker-controlled cadence deadline.
    HeartbeatDue {
        /// Full schedule returned by the interpreter.
        schedule: ShareGroupHeartbeatSchedule,
        /// Current monotonic observation proving cadence expiry.
        now: Moment,
    },
    /// Applies one successful join or steady heartbeat response.
    HeartbeatSucceeded {
        /// Exact in-flight request identity.
        attempt: ShareGroupHeartbeatAttempt,
        /// Current monotonic response observation.
        now: Moment,
        /// Positive coordinator-issued member epoch.
        member_epoch: ShareGroupMemberEpoch,
        /// Positive broker-controlled heartbeat interval.
        heartbeat_interval_ticks: u64,
        /// Nonnegative broker quota delay.
        throttle_ticks: u64,
        /// Ordered assignment when supplied by Kafka.
        assignment: Option<Vec<GroupAssignmentPartition>>,
    },
    /// Applies one terminal join or steady heartbeat failure.
    HeartbeatFailed {
        /// Exact in-flight request identity.
        attempt: ShareGroupHeartbeatAttempt,
        /// Stable normalized failure category.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Replaces an exact attempt after share-coordinator loss.
    RetryHeartbeat {
        /// Exact rejected request identity.
        attempt: ShareGroupHeartbeatAttempt,
        /// Current monotonic observation.
        now: Moment,
        /// Exact coordinator loss fact.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Schedules an exact attempt after coordinator loading.
    RetryCoordinatorLoad {
        /// Exact rejected request identity.
        attempt: ShareGroupHeartbeatAttempt,
        /// Current monotonic observation.
        now: Moment,
        /// Exact broker response.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Observes one exact heartbeat retry delay.
    RetryDue {
        /// Full core-issued retry schedule.
        schedule: ShareGroupHeartbeatRetrySchedule,
        /// Current monotonic observation.
        now: Moment,
    },
    /// Terminalizes a replacement whose route invalidation failed.
    RediscoveryFailed {
        /// Exact schedule paired with invalidation.
        schedule: ShareGroupHeartbeatRetrySchedule,
        /// Stable invalidation failure.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Rejoins an exact fenced steady member with epoch zero.
    RecoverFencedMembership {
        /// Exact rejected steady request.
        attempt: ShareGroupHeartbeatAttempt,
        /// Current monotonic observation.
        now: Moment,
        /// Exact fencing response.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Begins an epoch-minus-one leave under a public close deadline.
    BeginLeave {
        /// Current monotonic observation.
        now: Moment,
        /// Original absolute close deadline.
        deadline: Deadline,
    },
    /// Confirms successful leave settlement.
    LeaveSucceeded {
        /// Exact in-flight leave request.
        attempt: ShareGroupHeartbeatAttempt,
    },
    /// Applies a terminal leave failure.
    LeaveFailed {
        /// Exact in-flight leave request.
        attempt: ShareGroupHeartbeatAttempt,
        /// Stable normalized failure.
        failure: ShareGroupHeartbeatFailure,
    },
    /// Closes local ownership after no driver operation can complete.
    Close,
}
