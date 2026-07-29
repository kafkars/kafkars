//! Stable generated-free terminals for Admin `DescribeMetadataQuorum`.

use core::fmt;

use super::DescribeMetadataQuorumDescription;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact top-level `DescribeQuorum` Kafka rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeMetadataQuorumBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact fixed metadata-partition Kafka rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumPartitionError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeMetadataQuorumPartitionError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumFailure {
    pub(super) kind: DescribeMetadataQuorumFailureKind,
    pub(super) delivery: DescribeMetadataQuorumDeliveryStatus,
}

impl DescribeMetadataQuorumFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeMetadataQuorumFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeMetadataQuorumDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumOutcome {
    /// Kafka described the fixed metadata quorum.
    Described(DescribeMetadataQuorumDescription),
    /// Kafka rejected the complete request.
    BrokerRejected(DescribeMetadataQuorumBrokerError),
    /// Kafka rejected the fixed metadata partition.
    PartitionRejected(DescribeMetadataQuorumPartitionError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeMetadataQuorumFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeMetadataQuorumObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeMetadataQuorum result was already observed",
            Self::Stale => "Admin DescribeMetadataQuorum observer is stale",
        })
    }
}

impl std::error::Error for DescribeMetadataQuorumObserverError {}
