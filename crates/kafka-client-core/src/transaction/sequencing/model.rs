//! Closed identities, states, settlements, and failures for transactional sequencing.

use crate::{PartitionIndex, TopicId, TransactionEpoch};

/// Exact engine-catalog partition whose Kafka sequence is transaction-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionPartition {
    topic_id: TopicId,
    partition: PartitionIndex,
}

impl TransactionPartition {
    /// Joins one stable topic identity with one validated partition index.
    pub const fn new(topic_id: TopicId, partition: PartitionIndex) -> Self {
        Self {
            topic_id,
            partition,
        }
    }

    /// Returns the engine-catalog topic identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the validated Kafka partition index.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }
}

/// Coarse phase of one producer-identity-bound sequence owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSequenceState {
    /// No transaction owns sequence admission.
    Idle,
    /// One exact transaction epoch may acquire leases.
    Active(TransactionEpoch),
    /// Sequence certainty or the producer identity was lost permanently.
    Fenced,
}

/// Append certainty for one driver-accepted transactional Produce terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSequenceSettlement {
    /// Kafka appended the exact batch and consumed its sequence range.
    Succeeded,
    /// Kafka definitely did not append the exact batch.
    NotAppended,
    /// The final sequence position cannot be proven.
    Uncertain,
}

/// Deterministic rejection without partial sequence mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSequenceMachineError {
    /// The owner cannot bound any partition state.
    ZeroCapacity,
    /// Another transaction epoch already owns admission.
    AlreadyActive,
    /// No transaction epoch currently owns admission.
    NotActive,
    /// The supplied epoch is not the active or lease-owning epoch.
    EpochMismatch,
    /// Exact sequence leases must settle before the epoch can close.
    OutstandingLeases,
    /// Producer identity or sequence certainty was lost permanently.
    Fenced,
    /// This partition already has one exact outstanding lease.
    PartitionBusy,
    /// A new distinct partition would exceed the fixed lifetime envelope.
    PartitionCapacity,
    /// The requested record count is empty or not representable.
    InvalidRecordCount,
    /// Partition, epoch, or sequence range does not identify the retained lease.
    LeaseMismatch,
}
