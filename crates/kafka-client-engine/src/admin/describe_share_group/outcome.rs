//! Stable generated-free terminals for Admin `DescribeShareGroup`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

use super::DescribeShareGroupResult;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level Kafka rejection and its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeShareGroupBrokerError {
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

/// Exact result for one share group in a caller-ordered batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupBatchOutcome {
    /// Kafka returned one exact correlated group description.
    Described(DescribeShareGroupResult),
    /// Kafka rejected this specific share group.
    BrokerRejected {
        /// Exact requested share-group identity.
        group_id: String,
        /// Exact signed broker rejection.
        error: DescribeShareGroupBrokerError,
    },
}

impl DescribeShareGroupBatchOutcome {
    /// Returns the exact requested share-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Described(result) => {
                let description = result.description();
                &description.group_id
            }
            Self::BrokerRejected { group_id, .. } => group_id,
        }
    }
}

/// Caller-ordered batch result plus maximum observed broker throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeShareGroupBatchOutcome>,
}

impl DescribeShareGroupsBatch {
    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[DescribeShareGroupBatchOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<DescribeShareGroupBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category outside exact Kafka rejections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupFailureKind {
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
pub struct DescribeShareGroupFailure {
    pub(super) kind: DescribeShareGroupFailureKind,
    pub(super) delivery: DescribeShareGroupDeliveryStatus,
}

impl DescribeShareGroupFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeShareGroupFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeShareGroupDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupOutcome {
    /// Kafka returned one exact share-group description.
    Described(DescribeShareGroupResult),
    /// Kafka rejected the group-level operation.
    BrokerRejected(DescribeShareGroupBrokerError),
    /// Every requested group settled in original caller order.
    Batch(DescribeShareGroupsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeShareGroupFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeShareGroupObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeShareGroup result was already observed",
            Self::Stale => "Admin DescribeShareGroup observer is stale",
        })
    }
}

impl std::error::Error for DescribeShareGroupObserverError {}
