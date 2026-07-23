//! Opaque producer work identities and validated routing facts without record bytes.

use crate::ByteCount;

/// Identity of engine-owned record bytes and encoding metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PayloadId(u64);

impl PayloadId {
    /// Creates an opaque engine payload identity.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw deterministic identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Identity of a topic retained by the engine catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(u64);

impl TopicId {
    /// Creates an opaque topic-catalog identity.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw deterministic identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validated zero-based Kafka partition index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionIndex(u32);

impl PartitionIndex {
    /// Creates an index after public validation has established its range.
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Returns the zero-based index.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Identity of an engine-materialized record batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId(u64);

impl BatchId {
    /// Creates an opaque engine batch identity.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw deterministic identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fencing one immutable sealed-batch membership snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchExecutionGeneration(u64);

impl BatchExecutionGeneration {
    /// Restores a nonzero execution generation from persisted state.
    pub const fn try_from_raw(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the first generation assigned when a logical batch seals.
    pub const fn initial() -> Self {
        Self(1)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Exact identity of one immutable execution of a logical producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchExecutionId {
    batch_id: BatchId,
    generation: BatchExecutionGeneration,
}

impl BatchExecutionId {
    /// Joins one logical batch with its sealed-membership generation.
    pub const fn new(batch_id: BatchId, generation: BatchExecutionGeneration) -> Self {
        Self {
            batch_id,
            generation,
        }
    }

    /// Returns the logical batch retaining the membership.
    pub const fn batch_id(self) -> BatchId {
        self.batch_id
    }

    /// Returns the exact sealed-membership generation.
    pub const fn generation(self) -> BatchExecutionGeneration {
        self.generation
    }
}

/// Generation fencing replacement and cancellation of one batch timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchTimerGeneration(u64);

impl BatchTimerGeneration {
    /// Creates a deterministic timer generation.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw generation.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Explicit-partition record facts required by deterministic producer policy.
///
/// The engine resolves `payload_id` to the owned key, value, headers, timestamp,
/// and any encoding metadata. The core never retains those bytes or wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplicitRecord {
    payload_id: PayloadId,
    topic_id: TopicId,
    partition: PartitionIndex,
    retained_bytes: ByteCount,
}

impl ExplicitRecord {
    /// Creates validated, bytes-free record facts for producer admission.
    pub const fn new(
        payload_id: PayloadId,
        topic_id: TopicId,
        partition: PartitionIndex,
        retained_bytes: ByteCount,
    ) -> Self {
        Self {
            payload_id,
            topic_id,
            partition,
            retained_bytes,
        }
    }

    /// Returns the engine-owned payload identity.
    pub const fn payload_id(self) -> PayloadId {
        self.payload_id
    }

    /// Returns the engine topic-catalog identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the explicitly selected partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Returns bytes charged to deterministic admission policy.
    pub const fn retained_bytes(self) -> ByteCount {
        self.retained_bytes
    }
}
