//! Public exact-batch processed-prefix checkpoint ownership.

use crate::bridge::consumer_facade::group_consumer_batch::{
    GroupConsumerCheckpointBuilder as BridgeBuilder,
    GroupConsumerCheckpointMarkErrorKind as BridgeMarkErrorKind,
};

use super::{Checkpoint, GroupConsumerRecord};

/// Stable reason one record could not advance a partial checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointMarkErrorKind {
    /// The record was borrowed from a different consumer batch.
    ForeignRecord,
    /// The record was skipped, repeated, or supplied after a later record.
    OutOfOrder,
}

/// Rejected partial-checkpoint advancement without progress mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointMarkError {
    kind: CheckpointMarkErrorKind,
}

impl CheckpointMarkError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> CheckpointMarkErrorKind {
        self.kind
    }
}

impl std::fmt::Display for CheckpointMarkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "consumer checkpoint record rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for CheckpointMarkError {}

/// Ordered processed-prefix checkpoint tied to one exact consumer batch.
#[derive(Debug)]
#[must_use = "finish the builder to retain its processed-prefix checkpoint"]
pub struct CheckpointBuilder<'batch> {
    inner: BridgeBuilder<'batch>,
}

impl<'batch> CheckpointBuilder<'batch> {
    pub(super) const fn from_bridge(inner: BridgeBuilder<'batch>) -> Self {
        Self { inner }
    }

    /// Marks exactly the next record from this builder's source batch.
    ///
    /// Foreign, skipped, and repeated records return an error without advancing
    /// the checkpoint.
    pub fn mark_processed(
        &mut self,
        record: &GroupConsumerRecord<'_>,
    ) -> Result<(), CheckpointMarkError> {
        self.inner
            .mark_processed(record.as_bridge())
            .map_err(|kind| CheckpointMarkError {
                kind: match kind {
                    BridgeMarkErrorKind::ForeignRecord => CheckpointMarkErrorKind::ForeignRecord,
                    BridgeMarkErrorKind::OutOfOrder => CheckpointMarkErrorKind::OutOfOrder,
                },
            })
    }

    /// Finishes at the next offset after the last marked record.
    ///
    /// If no record was marked, the returned checkpoint names the first record
    /// and therefore skips none of the batch.
    pub fn finish(self) -> Checkpoint {
        Checkpoint::from_bridge(self.inner.finish())
    }
}
