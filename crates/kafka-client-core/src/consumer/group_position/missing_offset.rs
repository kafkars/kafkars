//! Closed missing-offset policy and exact reset-required ownership.

use super::{GroupPositionBatch, GroupPositionPartitionFact};
use crate::StartPosition;

/// Policy applied when `OffsetFetch` reports no committed next offset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupPositionMissingOffsetPolicy {
    /// Reject the complete assignment atomically.
    #[default]
    Error,
    /// Resolve each missing partition to Kafka's earliest available offset.
    Earliest,
    /// Resolve each missing partition to Kafka's latest available offset.
    Latest,
}

impl GroupPositionMissingOffsetPolicy {
    pub(crate) const fn reset_position(self) -> Option<StartPosition> {
        match self {
            Self::Error => None,
            Self::Earliest => Some(StartPosition::Beginning),
            Self::Latest => Some(StartPosition::End),
        }
    }
}

/// Full correlated `OffsetFetch` result requiring bounded `ListOffsets` work.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionMissingOffsetReset {
    batch: GroupPositionBatch,
    first_missing_index: usize,
    position: StartPosition,
}

impl GroupPositionMissingOffsetReset {
    pub(crate) const fn new(
        batch: GroupPositionBatch,
        first_missing_index: usize,
        position: StartPosition,
    ) -> Self {
        Self {
            batch,
            first_missing_index,
            position,
        }
    }

    /// Returns every exactly correlated committed and missing position fact.
    pub const fn batch(&self) -> &GroupPositionBatch {
        &self.batch
    }

    /// Returns the first missing partition in assignment order.
    pub fn first_missing(&self) -> GroupPositionPartitionFact {
        self.batch.facts()[self.first_missing_index]
    }

    /// Returns the exact earliest-or-latest position policy.
    pub const fn position(&self) -> StartPosition {
        self.position
    }

    /// Recovers the full ordered response and reset position.
    pub fn into_parts(self) -> (GroupPositionBatch, StartPosition) {
        (self.batch, self.position)
    }
}
