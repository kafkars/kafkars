//! Atomic ready, missing-offset, rejection, and failure decisions.

use super::{GroupPositionBatch, GroupPositionBrokerError, GroupPositionPartitionFact};

/// Whole-bootstrap failure outside an exactly correlated partition batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapFailureKind {
    /// The original absolute bootstrap deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected the request before accepting transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The selected `OffsetFetch` version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or did not correlate to the assignment.
    InvalidResponse,
    /// A structurally valid response exceeded retained capacity.
    ResponseTooLarge,
    /// Kafka rejected the complete group-level request.
    Broker(GroupPositionBrokerError),
}

/// One terminal whole-bootstrap failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapFailure {
    kind: GroupPositionBootstrapFailureKind,
}

impl GroupPositionBootstrapFailure {
    pub(crate) const fn new(kind: GroupPositionBootstrapFailureKind) -> Self {
        Self { kind }
    }

    /// Returns the exact terminal failure category.
    pub const fn kind(self) -> GroupPositionBootstrapFailureKind {
        self.kind
    }
}

/// Atomic Error-policy failure retaining all committed and missing facts.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapMissingOffsets {
    batch: GroupPositionBatch,
    first_missing_index: usize,
}

impl GroupPositionBootstrapMissingOffsets {
    pub(crate) const fn new(batch: GroupPositionBatch, first_missing_index: usize) -> Self {
        Self {
            batch,
            first_missing_index,
        }
    }

    /// Returns every exactly correlated position fact.
    pub const fn batch(&self) -> &GroupPositionBatch {
        &self.batch
    }

    /// Returns the first missing offset in assignment order.
    pub fn first_missing(&self) -> GroupPositionPartitionFact {
        self.batch.facts()[self.first_missing_index]
    }

    /// Recovers the full ordered response batch.
    pub fn into_batch(self) -> GroupPositionBatch {
        self.batch
    }
}

/// Atomic partition rejection retaining every correlated response fact.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapPartitionRejection {
    batch: GroupPositionBatch,
    first_rejected_index: usize,
}

impl GroupPositionBootstrapPartitionRejection {
    pub(crate) const fn new(batch: GroupPositionBatch, first_rejected_index: usize) -> Self {
        Self {
            batch,
            first_rejected_index,
        }
    }

    /// Returns every exactly correlated position fact.
    pub const fn batch(&self) -> &GroupPositionBatch {
        &self.batch
    }

    /// Returns the first partition rejection in assignment order.
    pub fn first_rejected(&self) -> GroupPositionPartitionFact {
        self.batch.facts()[self.first_rejected_index]
    }

    /// Recovers the full ordered response batch.
    pub fn into_batch(self) -> GroupPositionBatch {
        self.batch
    }
}

/// Exactly one terminal decision for one assignment bootstrap.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapTerminal {
    /// Every assigned partition has a committed next offset.
    Ready(GroupPositionBatch),
    /// Error policy rejected one or more missing committed offsets atomically.
    MissingOffsets(GroupPositionBootstrapMissingOffsets),
    /// Kafka rejected at least one exactly correlated partition.
    PartitionRejected(GroupPositionBootstrapPartitionRejection),
    /// The complete bootstrap failed outside correlated partition results.
    Failed(GroupPositionBootstrapFailure),
}
