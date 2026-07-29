//! Stable generated-free terminals for Admin `DescribeStreamsGroup`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

use super::DescribeStreamsGroupResult;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level Kafka rejection and its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeStreamsGroupBrokerError {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's optional bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether the diagnostic was truncated at the retained-byte bound.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the rejection into exact stable parts.
    pub fn into_parts(self) -> (u32, i16, Option<String>, bool) {
        (
            self.throttle_time_ms,
            self.code,
            self.message,
            self.message_truncated,
        )
    }
}

/// Stable whole-operation failure category outside exact Kafka rejections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Valid response facts exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupFailure {
    pub(super) kind: DescribeStreamsGroupFailureKind,
    pub(super) delivery: DescribeStreamsGroupDeliveryStatus,
}

impl DescribeStreamsGroupFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeStreamsGroupFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeStreamsGroupDeliveryStatus {
        self.delivery
    }
}

/// Exact result for one streams group in a caller-ordered batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupBatchOutcome {
    /// Kafka returned one exact streams-group description.
    Described(DescribeStreamsGroupResult),
    /// Kafka rejected this exact streams group.
    BrokerRejected {
        /// Exact requested streams-group identity.
        group_id: String,
        /// Exact signed rejection, throttle, and bounded diagnostic.
        error: DescribeStreamsGroupBrokerError,
    },
}

impl DescribeStreamsGroupBatchOutcome {
    /// Returns the exact requested streams-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Described(result) => result.description().group_id(),
            Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Returns this group's nonnegative Kafka throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        match self {
            Self::Described(result) => result.throttle_time_ms(),
            Self::BrokerRejected { error, .. } => error.throttle_time_ms(),
        }
    }
}

/// Caller-ordered outcomes for one accepted streams-group batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeStreamsGroupBatchOutcome>,
}

impl DescribeStreamsGroupsBatch {
    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[DescribeStreamsGroupBatchOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<DescribeStreamsGroupBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupOutcome {
    /// Kafka returned one exact streams-group description.
    Described(DescribeStreamsGroupResult),
    /// Kafka rejected the group-level operation.
    BrokerRejected(DescribeStreamsGroupBrokerError),
    /// Every requested group settled in exact caller order.
    Batch(DescribeStreamsGroupsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeStreamsGroupFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeStreamsGroupObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeStreamsGroup result was already observed",
            Self::Stale => "Admin DescribeStreamsGroup observer is stale",
        })
    }
}

impl std::error::Error for DescribeStreamsGroupObserverError {}
