//! Semantic `ListOffsets` inputs and results without generated wire ownership.

use core::num::NonZeroI16;

use kafka_client_core::NextFetchOffset;

/// Transactional visibility used while resolving an end position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListOffsetsIsolation {
    /// Observe the partition high watermark.
    ReadUncommitted,
    /// Observe the partition last stable offset.
    ReadCommitted,
}

impl ListOffsetsIsolation {
    pub(super) const fn wire_value(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

/// Valid successful position facts returned by one broker partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedPosition {
    next_offset: NextFetchOffset,
    timestamp_ms: Option<i64>,
    leader_epoch: Option<i32>,
}

impl ResolvedPosition {
    pub(super) const fn new(
        next_offset: NextFetchOffset,
        timestamp_ms: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            next_offset,
            timestamp_ms,
            leader_epoch,
        }
    }

    /// Returns the nonnegative offset to supply to core position policy.
    pub(crate) const fn next_offset(self) -> NextFetchOffset {
        self.next_offset
    }

    /// Returns Kafka's associated timestamp when it is known.
    pub(crate) const fn timestamp_ms(self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Returns Kafka's leader epoch when the selected version supplied one.
    pub(crate) const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// One correlated partition response without retry or invalidation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListOffsetsOutcome {
    /// Kafka resolved the requested beginning or end position.
    Resolved(ResolvedPosition),
    /// Kafka rejected the partition query with an exact signed code.
    BrokerError {
        /// Exact nonzero code, including unknown future values.
        code: NonZeroI16,
    },
}
