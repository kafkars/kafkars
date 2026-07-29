//! Stable generated-free terminals for Admin `AlterShareGroupOffsets`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

use super::AlterShareGroupOffsetsBatch;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level Kafka rejection and its bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsBrokerError {
    pub(super) throttle_time_ms: u32,
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl AlterShareGroupOffsetsBrokerError {
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
pub enum AlterShareGroupOffsetsFailureKind {
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
pub struct AlterShareGroupOffsetsFailure {
    pub(super) kind: AlterShareGroupOffsetsFailureKind,
    pub(super) delivery: AlterShareGroupOffsetsDeliveryStatus,
}

impl AlterShareGroupOffsetsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AlterShareGroupOffsetsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AlterShareGroupOffsetsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsOutcome {
    /// Kafka returned one result for every requested topic-partition.
    Altered(AlterShareGroupOffsetsBatch),
    /// Kafka rejected the group-level operation.
    BrokerRejected(AlterShareGroupOffsetsBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(AlterShareGroupOffsetsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AlterShareGroupOffsetsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin AlterShareGroupOffsets result was already observed",
            Self::Stale => "Admin AlterShareGroupOffsets observer is stale",
        })
    }
}

impl std::error::Error for AlterShareGroupOffsetsObserverError {}
