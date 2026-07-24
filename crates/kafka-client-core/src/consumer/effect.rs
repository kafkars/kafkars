//! Ordered direct-consumer actions for a future engine fetch interpreter.

use core::num::NonZeroI16;

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

/// Terminal reason successful Fetch progress cannot schedule its next fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchThrottleFailure {
    /// The positive throttle duration could not become an absolute deadline.
    DeadlineOverflow,
}

/// Terminal semantic reason one exact Fetch cannot deliver or advance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchFailure {
    /// The absolute Fetch deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected permanent ownership of the request.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed Fetch error code.
    Broker(NonZeroI16),
    /// The selected Fetch version cannot preserve required semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The generated or decoded response exceeded a configured bound.
    ResponseTooLarge,
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
    /// Arms one positive successful-Fetch throttle before the next fetch.
    ArmFetchThrottle {
        /// Exact future fetch fenced by the timer.
        fence: FetchFence,
        /// Exact absolute throttle deadline.
        deadline: Deadline,
    },
    /// Publishes terminal failure to schedule after one successful fetch.
    FetchThrottleFailed {
        /// Exact completed fetch whose throttle could not be represented.
        fence: FetchFence,
        /// Deterministic terminal classification.
        failure: FetchThrottleFailure,
    },
    /// Authorizes application delivery of the engine-owned records for one exact Fetch.
    ///
    /// The interpreter must apply this before later effects from the same
    /// transition. The supplied next offset is the checkpoint position after
    /// every record represented by that retained delivery.
    AuthorizeFetchDelivery {
        /// Exact Fetch whose retained records may become application-visible.
        fence: FetchFence,
        /// Assignment-fenced next offset carried by the delivery checkpoint.
        next_offset: NextFetchOffset,
    },
    /// Publishes terminal failure of one exact Fetch without retry.
    FetchFailed {
        /// Exact Fetch execution whose attempt terminated.
        fence: FetchFence,
        /// Preserved semantic failure selected by the engine protocol boundary.
        failure: FetchFailure,
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
