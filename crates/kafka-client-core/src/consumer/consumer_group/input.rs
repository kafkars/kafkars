//! Normalized facts accepted by one KIP-848 membership owner.

use crate::{AssignmentGeneration, Deadline, GroupAssignmentPartition, MemberId, Moment};

use super::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatFailure,
    ConsumerGroupHeartbeatRetrySchedule, ConsumerGroupHeartbeatSchedule, ConsumerGroupMemberEpoch,
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
    /// Confirms exact engine retirement of the still-reportable prior assignment.
    AssignmentRetired {
        /// Current monotonic observation starting the immediate acknowledgement attempt.
        now: Moment,
        /// Exact retained member identity.
        member_id: MemberId,
        /// Exact broker epoch paired with the pending target.
        member_epoch: ConsumerGroupMemberEpoch,
        /// Exact prior assignment generation whose ownership is now empty.
        assignment_generation: AssignmentGeneration,
    },
    /// Applies one exact terminal join or steady heartbeat failure.
    HeartbeatFailed {
        /// Exact in-flight request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Stable normalized failure category.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Requests one bounded replacement of the exact in-flight heartbeat after coordinator loss.
    RetryHeartbeat {
        /// Exact rejected request identity; each replacement allocates a fresh attempt.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Current monotonic observation used against the original attempt deadline.
        now: Moment,
        /// Normalized failure; only exact coordinator-unavailability categories authorize replacement.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Terminalizes a replacement whose paired route invalidation failed.
    RediscoveryFailed {
        /// Exact retained retry schedule paired with the invalidation.
        schedule: ConsumerGroupHeartbeatRetrySchedule,
        /// Stable normalized invalidation failure.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Schedules the exact in-flight request after Kafka reports a loading coordinator.
    RetryCoordinatorLoad {
        /// Exact in-flight Join, Steady, or Leave request identity.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Current monotonic observation used against the original attempt deadline.
        now: Moment,
        /// Exact broker response; only `COORDINATOR_LOAD_IN_PROGRESS` authorizes this retry.
        failure: ConsumerGroupHeartbeatFailure,
    },
    /// Observes one exact coordinator-load backoff deadline.
    CoordinatorLoadRetryDue {
        /// Full core-issued retry schedule.
        schedule: ConsumerGroupHeartbeatRetrySchedule,
        /// Current monotonic observation proving backoff expiry.
        now: Moment,
    },
    /// Abandons a fenced steady membership and rejoins with the retained member identity.
    RecoverFencedMembership {
        /// Exact in-flight steady request identity that received the fencing response.
        attempt: ConsumerGroupHeartbeatAttempt,
        /// Current monotonic observation supplied by the interpreter.
        now: Moment,
        /// Exact fencing response; only unknown-member and fenced-epoch broker codes recover.
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
