//! Exact broker errors and terminal facts for metadata-quorum description.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeMetadataQuorumDescription;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const DESCRIBE_METADATA_QUORUM_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact top-level `DescribeQuorum` Kafka rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeMetadataQuorumBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact fixed metadata-partition Kafka rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumPartitionError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeMetadataQuorumPartitionError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes the error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Whole-operation failure outside exact Kafka broker rejections.
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

/// Whole-operation mechanism failure with exact delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumFailure {
    kind: DescribeMetadataQuorumFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeMetadataQuorumFailure {
    pub(crate) const fn new(
        kind: DescribeMetadataQuorumFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeMetadataQuorumFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeMetadataQuorum`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumTerminal {
    /// Kafka described the fixed metadata quorum.
    Described(DescribeMetadataQuorumDescription),
    /// Kafka rejected the complete request at the response top level.
    BrokerRejected(DescribeMetadataQuorumBrokerError),
    /// Kafka rejected the fixed metadata partition.
    PartitionRejected(DescribeMetadataQuorumPartitionError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeMetadataQuorumFailure),
}
