//! Stable Rust vocabulary for directly assigned consumer failure events.

/// Exact named assignment and position generation for one failure event.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerPositionFence {
    topic: String,
    partition: i32,
    assignment_epoch: u64,
    position_epoch: u64,
}

impl AssignedConsumerPositionFence {
    pub(crate) const fn from_parts(
        topic: String,
        partition: i32,
        assignment_epoch: u64,
        position_epoch: u64,
    ) -> Self {
        Self {
            topic,
            partition,
            assignment_epoch,
            position_epoch,
        }
    }

    /// Returns the exact Kafka topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the complete direct-assignment generation.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }

    /// Returns the partition-local position generation.
    pub const fn position_epoch(&self) -> u64 {
        self.position_epoch
    }
}

/// Exact named identity of one failed Fetch execution.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerFetchFence {
    position: AssignedConsumerPositionFence,
    fetch_revision: u64,
}

impl AssignedConsumerFetchFence {
    pub(crate) const fn from_parts(
        position: AssignedConsumerPositionFence,
        fetch_revision: u64,
    ) -> Self {
        Self {
            position,
            fetch_revision,
        }
    }

    /// Borrows the exact named position generation.
    pub const fn position(&self) -> &AssignedConsumerPositionFence {
        &self.position
    }

    /// Returns the partition-local Fetch revision.
    pub const fn fetch_revision(&self) -> u64 {
        self.fetch_revision
    }
}

/// Stable reason one position-resolution attempt terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerPositionResolutionFailureKind {
    /// The original resolution deadline elapsed.
    DeadlineElapsed,
    /// The engine driver rejected permanent request ownership.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed error code.
    Broker(i16),
    /// The selected `ListOffsets` version could not preserve requested semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The generated or decoded response exceeded a configured bound.
    ResponseTooLarge,
    /// A positive broker throttle could not become an absolute deadline.
    ThrottleDeadlineOverflow,
}

/// Stable reason the next Fetch could not be scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerFetchThrottleFailureKind {
    /// A positive broker throttle could not become an absolute deadline.
    DeadlineOverflow,
}

/// Stable reason one exact Fetch terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerFetchFailureKind {
    /// The absolute Fetch deadline elapsed.
    DeadlineElapsed,
    /// The engine driver rejected permanent request ownership.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed error code.
    Broker(i16),
    /// The selected Fetch version could not preserve required semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The response exceeded a configured retained-data bound.
    ResponseTooLarge,
}

/// One application-visible failure transferred from the bounded event FIFO.
#[derive(Debug, Eq, PartialEq)]
pub enum AssignedConsumerEvent {
    /// One exact position resolution terminated.
    PositionResolutionFailed {
        /// Exact named position generation.
        fence: AssignedConsumerPositionFence,
        /// Stable terminal category.
        kind: AssignedConsumerPositionResolutionFailureKind,
    },
    /// Scheduling after one successful Fetch terminated.
    FetchThrottleFailed {
        /// Exact named Fetch generation.
        fence: AssignedConsumerFetchFence,
        /// Stable terminal category.
        kind: AssignedConsumerFetchThrottleFailureKind,
    },
    /// One exact Fetch execution terminated.
    FetchFailed {
        /// Exact named Fetch generation.
        fence: AssignedConsumerFetchFence,
        /// Stable terminal category and exact broker code.
        kind: AssignedConsumerFetchFailureKind,
    },
}
