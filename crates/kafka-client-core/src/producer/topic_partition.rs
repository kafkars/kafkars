//! Scalar topic metadata facts and generation-fenced partition decisions.

use core::fmt;

use crate::PartitionIndex;

/// Driver-local generation of one immutable topic metadata observation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicMetadataGeneration(u64);

impl TopicMetadataGeneration {
    /// Preserves the generation assigned by the topology authority.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the topology authority's generation value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Nonnegative broker-issued epoch for one known partition leader.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaderEpoch(i32);

impl LeaderEpoch {
    /// Normalizes Kafka's absent `-1` sentinel and rejects malformed lower values.
    pub const fn try_from_raw(value: i32) -> Result<Option<Self>, LeaderEpochError> {
        match value {
            -1 => Ok(None),
            0.. => Ok(Some(Self(value))),
            _ => Err(LeaderEpochError { value }),
        }
    }

    /// Returns the broker-issued epoch.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Rejection of a leader epoch below Kafka's absent `-1` sentinel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderEpochError {
    value: i32,
}

impl LeaderEpochError {
    /// Returns the rejected signed epoch.
    pub const fn value(self) -> i32 {
        self.value
    }
}

impl fmt::Display for LeaderEpochError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "leader epoch {} is below Kafka's absent -1 sentinel",
            self.value
        )
    }
}

impl std::error::Error for LeaderEpochError {}

/// One partition that currently has a driver-known leader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvailablePartition {
    partition: PartitionIndex,
    leader_epoch: Option<LeaderEpoch>,
}

impl AvailablePartition {
    /// Creates one available partition from normalized topology facts.
    pub const fn new(partition: PartitionIndex, leader_epoch: Option<LeaderEpoch>) -> Self {
        Self {
            partition,
            leader_epoch,
        }
    }

    /// Returns the zero-based partition index.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Returns the known leader epoch when metadata supplied one.
    pub const fn leader_epoch(self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }
}

/// One partition decision fenced by the metadata facts that produced it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionSelection {
    generation: TopicMetadataGeneration,
    partition: PartitionIndex,
    available: bool,
    leader_epoch: Option<LeaderEpoch>,
}

impl PartitionSelection {
    pub(super) const fn available(
        generation: TopicMetadataGeneration,
        fact: AvailablePartition,
    ) -> Self {
        Self {
            generation,
            partition: fact.partition(),
            available: true,
            leader_epoch: fact.leader_epoch(),
        }
    }

    pub(super) const fn unavailable(
        generation: TopicMetadataGeneration,
        partition: PartitionIndex,
    ) -> Self {
        Self {
            generation,
            partition,
            available: false,
            leader_epoch: None,
        }
    }

    /// Returns the metadata generation used by partition policy.
    pub const fn generation(self) -> TopicMetadataGeneration {
        self.generation
    }

    /// Returns the selected zero-based partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Reports whether the source view had a known leader for this partition.
    pub const fn is_available(self) -> bool {
        self.available
    }

    /// Returns the selected partition's known leader epoch, when present.
    pub const fn leader_epoch(self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }
}
