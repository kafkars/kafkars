//! Stable generated-free terminals for Admin `DescribeTopicPartitions`.

use core::fmt;

mod page;
mod partition;
mod topic;
mod translate;

pub use page::AdminDescribeTopicPartitionsPage;
pub use partition::AdminDescribeTopicPartition;
pub use topic::AdminDescribeTopicPartitionsTopic;
pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsFailureKind {
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
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTopicPartitionsFailure {
    pub(super) kind: AdminDescribeTopicPartitionsFailureKind,
    pub(super) delivery: AdminDescribeTopicPartitionsDeliveryStatus,
}

impl AdminDescribeTopicPartitionsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeTopicPartitionsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AdminDescribeTopicPartitionsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned one-page terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsOutcome {
    /// Kafka returned one explicit page and optional next cursor.
    Page(AdminDescribeTopicPartitionsPage),
    /// Execution failed outside per-topic and per-partition broker facts.
    Failed(AdminDescribeTopicPartitionsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminDescribeTopicPartitionsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeTopicPartitions result was already observed",
            Self::Stale => "Admin DescribeTopicPartitions observer is stale",
        })
    }
}

impl std::error::Error for AdminDescribeTopicPartitionsObserverError {}
