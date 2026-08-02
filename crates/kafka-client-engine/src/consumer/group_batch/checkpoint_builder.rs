//! Exact-batch prefix tracking for assignment-fenced group checkpoints.

use super::{
    GroupConsumerCheckpoint, GroupConsumerRecord, GroupConsumerRecords,
    checkpoint::checkpoint_from_delivery_at,
};
use crate::consumer::group::ClassicGroupFetchDelivery;

/// Stable reason a record could not advance one partial checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCheckpointMarkErrorKind {
    /// The record view belongs to a different retained batch.
    ForeignRecord,
    /// The record is not the next unprocessed record in this batch.
    OutOfOrder,
}

/// Rejected partial-checkpoint advancement without progress mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerCheckpointMarkError {
    kind: GroupConsumerCheckpointMarkErrorKind,
}

impl GroupConsumerCheckpointMarkError {
    const fn new(kind: GroupConsumerCheckpointMarkErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> GroupConsumerCheckpointMarkErrorKind {
        self.kind
    }
}

impl std::fmt::Display for GroupConsumerCheckpointMarkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "group checkpoint record rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerCheckpointMarkError {}

/// Ordered processed-prefix owner tied to one exact retained group batch.
#[must_use = "finish the builder to retain its processed-prefix checkpoint"]
pub struct GroupConsumerCheckpointBuilder<'batch> {
    delivery: &'batch ClassicGroupFetchDelivery,
    next_ordinal: usize,
    next_offset: i64,
}

impl<'batch> GroupConsumerCheckpointBuilder<'batch> {
    pub(super) fn new(delivery: &'batch ClassicGroupFetchDelivery) -> Self {
        let next_offset = GroupConsumerRecords::new(delivery)
            .next()
            .map_or_else(|| delivery.next_offset().get(), |record| record.offset());
        Self {
            delivery,
            next_ordinal: 0,
            next_offset,
        }
    }

    /// Advances through exactly the next record from this batch.
    ///
    /// A foreign, skipped, or repeated record returns an error without changing
    /// the retained prefix.
    pub fn mark_processed(
        &mut self,
        record: &GroupConsumerRecord<'_>,
    ) -> Result<(), GroupConsumerCheckpointMarkError> {
        if !record.belongs_to(self.delivery) {
            return Err(GroupConsumerCheckpointMarkError::new(
                GroupConsumerCheckpointMarkErrorKind::ForeignRecord,
            ));
        }
        if record.ordinal() != self.next_ordinal {
            return Err(GroupConsumerCheckpointMarkError::new(
                GroupConsumerCheckpointMarkErrorKind::OutOfOrder,
            ));
        }
        let next_offset = record
            .offset()
            .checked_add(1)
            .unwrap_or_else(|| unreachable!("Fetch normalization rejects next-offset overflow"));
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.next_offset = next_offset;
        Ok(())
    }

    /// Finishes at the next offset after the last marked record.
    ///
    /// With no marked records, the checkpoint names the first application
    /// record so committing it skips none of this batch.
    pub fn finish(self) -> GroupConsumerCheckpoint {
        checkpoint_from_delivery_at(self.delivery, self.next_offset)
    }
}

impl std::fmt::Debug for GroupConsumerCheckpointBuilder<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerCheckpointBuilder")
            .field("topic", &self.delivery.topic())
            .field("partition", &self.delivery.partition())
            .field("next_ordinal", &self.next_ordinal)
            .field("next_offset", &self.next_offset)
            .finish()
    }
}
