//! Ordered direct-consumer actions for a future engine fetch interpreter.

use super::{
    AssignedTopicPartition, AssignmentEpoch, FetchFence, NextFetchOffset, PositionFence,
    StartPosition,
};
use crate::Deadline;

/// Terminal reason one position-resolution attempt cannot become fetch-ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionResolutionFailure {
    /// The supplied public operation deadline elapsed.
    DeadlineElapsed,
    /// The interpreter reported terminal resolution failure.
    AttemptFailed,
    /// The positive throttle duration could not become an absolute deadline.
    ThrottleDeadlineOverflow,
}

/// One ordered action selected by deterministic direct-consumer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerEffect {
    /// Terminally cancels all internal interpreter work for a partition.
    Revoke {
        /// Superseded assignment epoch.
        assignment_epoch: AssignmentEpoch,
        /// Superseded partition.
        partition: AssignedTopicPartition,
    },
    /// Cancels interpreter work older than the supplied position fence.
    ///
    /// Cancellation is terminal for an outstanding internal resolution or
    /// throttle effect; it does not produce a public terminal result.
    Suspend {
        /// Newly installed fence; older work must not be applied.
        fence: PositionFence,
    },
    /// Resolves an earliest or end-offset start position.
    ResolvePosition {
        /// Exact generation of the resolution request.
        fence: PositionFence,
        /// Beginning or end policy to resolve.
        position: StartPosition,
        /// Original absolute deadline supplied at the public operation boundary.
        deadline: Deadline,
    },
    /// Publishes terminal failure of one exact position resolution.
    PositionResolutionFailed {
        /// Exact generation whose resolution terminated.
        fence: PositionFence,
        /// Deterministic terminal classification.
        failure: PositionResolutionFailure,
    },
    /// Arms one positive broker throttle before fetch readiness.
    ArmPositionThrottle {
        /// Exact position fenced by the timer.
        fence: PositionFence,
        /// Exact absolute throttle deadline.
        deadline: Deadline,
    },
    /// Announces that one exact partition position may be fetched.
    FetchReady {
        /// Exact execution identity for the fetch.
        fence: FetchFence,
        /// Offset used by this fetch.
        next_offset: NextFetchOffset,
    },
}

/// Ordered output of one accepted direct-consumer transition.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerTransition {
    assignment_epoch: AssignmentEpoch,
    effects: Vec<AssignedConsumerEffect>,
}

impl AssignedConsumerTransition {
    pub(crate) const fn new(
        assignment_epoch: AssignmentEpoch,
        effects: Vec<AssignedConsumerEffect>,
    ) -> Self {
        Self {
            assignment_epoch,
            effects,
        }
    }

    /// Returns the active assignment generation after the transition.
    pub const fn assignment_epoch(&self) -> AssignmentEpoch {
        self.assignment_epoch
    }

    /// Borrows interpreter actions in deterministic execution order.
    pub fn effects(&self) -> &[AssignedConsumerEffect] {
        &self.effects
    }

    /// Moves the ordered actions into a future interpreter.
    pub fn into_effects(self) -> Vec<AssignedConsumerEffect> {
        self.effects
    }
}
