//! Stable generated-free singular and batch terminals for `ListShareGroupOffsets`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

use super::ListShareGroupOffsetsBatch;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level Kafka rejection and its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl ListShareGroupOffsetsBrokerError {
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
pub enum ListShareGroupOffsetsBatchOutcome {
    /// Kafka returned correlated partition outcomes for this group.
    Offsets {
        /// Exact requested share-group identity.
        group_id: String,
        /// Canonically or caller-ordered partition outcomes.
        offsets: ListShareGroupOffsetsBatch,
    },
    /// Kafka rejected this specific share group.
    BrokerRejected {
        /// Exact requested share-group identity.
        group_id: String,
        /// Exact signed group rejection.
        error: ListShareGroupOffsetsBrokerError,
    },
}

impl ListShareGroupOffsetsBatchOutcome {
    /// Returns the exact requested share-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Offsets { group_id, .. } | Self::BrokerRejected { group_id, .. } => group_id,
        }
    }
}

/// Caller-ordered multi-group result plus maximum observed broker throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupsOffsetsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<ListShareGroupOffsetsBatchOutcome>,
}

impl ListShareGroupsOffsetsBatch {
    /// Returns the maximum throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[ListShareGroupOffsetsBatchOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into maximum throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<ListShareGroupOffsetsBatchOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category outside exact Kafka rejections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsFailureKind {
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
pub struct ListShareGroupOffsetsFailure {
    pub(super) kind: ListShareGroupOffsetsFailureKind,
    pub(super) delivery: ListShareGroupOffsetsDeliveryStatus,
}

impl ListShareGroupOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> ListShareGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> ListShareGroupOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsOutcome {
    /// Kafka returned one result for every selected or visible partition.
    Offsets(ListShareGroupOffsetsBatch),
    /// Kafka rejected the group-level operation.
    BrokerRejected(ListShareGroupOffsetsBrokerError),
    /// Every requested group settled in original caller order.
    Batch(ListShareGroupsOffsetsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(ListShareGroupOffsetsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for ListShareGroupOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin ListShareGroupOffsets result was already observed",
            Self::Stale => "Admin ListShareGroupOffsets observer is stale",
        })
    }
}

impl std::error::Error for ListShareGroupOffsetsObserverError {}
