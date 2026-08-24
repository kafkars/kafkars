//! Stable scalar events independent of deterministic core representations.

use std::sync::Arc;

/// Exact named assignment and position generation for one failure event.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerPositionFence {
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
    pub(super) assignment_epoch: u64,
    pub(super) position_epoch: u64,
}

impl AssignedConsumerPositionFence {
    /// Returns the catalog-owned Kafka topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the partition acquisition generation retained by this fence.
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
    pub(super) position: AssignedConsumerPositionFence,
    pub(super) fetch_revision: u64,
}

impl AssignedConsumerFetchFence {
    /// Borrows the exact named position generation.
    pub const fn position(&self) -> &AssignedConsumerPositionFence {
        &self.position
    }

    /// Returns the partition-local Fetch revision.
    pub const fn fetch_revision(&self) -> u64 {
        self.fetch_revision
    }
}

/// Stable terminal reason one position resolution failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerPositionResolutionFailureKind {
    /// The original resolution deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected permanent request ownership.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed error code.
    Broker(i16),
    /// The selected `ListOffsets` version could not preserve required semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The generated or decoded response exceeded a configured bound.
    ResponseTooLarge,
    /// A positive throttle could not become an absolute deadline.
    ThrottleDeadlineOverflow,
}

/// One exact terminal position-resolution event.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerPositionResolutionFailure {
    pub(super) fence: AssignedConsumerPositionFence,
    pub(super) kind: AssignedConsumerPositionResolutionFailureKind,
}

impl AssignedConsumerPositionResolutionFailure {
    /// Borrows the exact named position generation.
    pub const fn fence(&self) -> &AssignedConsumerPositionFence {
        &self.fence
    }

    /// Returns the normalized terminal category.
    pub const fn kind(&self) -> AssignedConsumerPositionResolutionFailureKind {
        self.kind
    }
}

/// Stable terminal reason the next Fetch could not be scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerFetchThrottleFailureKind {
    /// A positive broker throttle could not become an absolute deadline.
    DeadlineOverflow,
}

/// One exact terminal successful-Fetch throttle event.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerFetchThrottleFailure {
    pub(super) fence: AssignedConsumerFetchFence,
    pub(super) kind: AssignedConsumerFetchThrottleFailureKind,
}

impl AssignedConsumerFetchThrottleFailure {
    /// Borrows the exact named Fetch generation.
    pub const fn fence(&self) -> &AssignedConsumerFetchFence {
        &self.fence
    }

    /// Returns the normalized terminal category.
    pub const fn kind(&self) -> AssignedConsumerFetchThrottleFailureKind {
        self.kind
    }
}

/// Stable terminal reason one exact Fetch failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerFetchFailureKind {
    /// The absolute Fetch deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected permanent request ownership.
    DriverRejected,
    /// Transport ownership terminated without a response.
    Transport,
    /// Kafka returned one exact nonzero signed error code.
    Broker(i16),
    /// The selected Fetch version could not preserve required semantics.
    Compatibility,
    /// The correlated response was structurally or semantically invalid.
    InvalidResponse,
    /// The generated or decoded response exceeded a configured bound.
    ResponseTooLarge,
}

/// One exact terminal Fetch event.
#[derive(Debug, Eq, PartialEq)]
pub struct AssignedConsumerFetchFailure {
    pub(super) fence: AssignedConsumerFetchFence,
    pub(super) kind: AssignedConsumerFetchFailureKind,
}

impl AssignedConsumerFetchFailure {
    /// Borrows the exact named Fetch generation.
    pub const fn fence(&self) -> &AssignedConsumerFetchFence {
        &self.fence
    }

    /// Returns the normalized terminal category and exact broker code.
    pub const fn kind(&self) -> AssignedConsumerFetchFailureKind {
        self.kind
    }
}

/// One application-visible failure transferred from the bounded FIFO.
#[derive(Debug, Eq, PartialEq)]
pub enum AssignedConsumerEvent {
    /// One terminal position resolution.
    PositionResolutionFailed(AssignedConsumerPositionResolutionFailure),
    /// One terminal failure to schedule after a successful Fetch.
    FetchThrottleFailed(AssignedConsumerFetchThrottleFailure),
    /// One terminal Fetch execution.
    FetchFailed(AssignedConsumerFetchFailure),
}
