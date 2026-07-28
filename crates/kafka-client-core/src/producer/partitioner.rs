//! Java-compatible keyed partition selection over serialized record-key bytes.

use core::fmt;

use crate::PartitionIndex;

use super::{PartitionSelection, TopicPartitionFacts, TopicPartitionFactsError};

const JAVA_SIGNED_INT_MAX: u32 = i32::MAX.unsigned_abs();
const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_MULTIPLIER: u32 = 0x5bd1_e995;

/// A nonzero topic partition count representable by Kafka's Java client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionCount(u32);

impl PartitionCount {
    /// Validates a total logical partition count.
    pub const fn try_from_raw(value: u32) -> Option<Self> {
        if value == 0 || value > JAVA_SIGNED_INT_MAX {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Returns the validated partition count.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Failure to apply Java-compatible keyed partition selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyedPartitionError {
    /// The serialized key length exceeds Java's signed array-length domain.
    KeyLengthUnrepresentable,
    /// The supplied lazy topic view violated its normalized fact contract.
    IncoherentTopicFacts(TopicPartitionFactsError),
}

impl fmt::Display for KeyedPartitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyLengthUnrepresentable => {
                formatter.write_str("serialized key length exceeds Java's signed array domain")
            }
            Self::IncoherentTopicFacts(source) => {
                write!(formatter, "incoherent topic partition facts: {source}")
            }
        }
    }
}

impl std::error::Error for KeyedPartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::KeyLengthUnrepresentable => None,
            Self::IncoherentTopicFacts(source) => Some(source),
        }
    }
}

/// Selects a partition exactly as Kafka's Java default keyed path does.
///
/// The caller supplies the total logical topic partition count, including
/// partitions that are temporarily unavailable. An absent key belongs to the
/// separate unkeyed policy; an empty but present key is valid input here.
pub fn select_java_keyed_partition(
    serialized_key: &[u8],
    partition_count: PartitionCount,
) -> Result<PartitionIndex, KeyedPartitionError> {
    let key_length = java_key_length(serialized_key.len())?;
    let hash = murmur2(serialized_key, key_length) & i32::MAX.unsigned_abs();
    Ok(PartitionIndex::from_raw(hash % partition_count.get()))
}

/// Selects a generation-stamped topic partition through the Java-compatible keyed path.
///
/// Hashing uses the total logical partition count, including partitions that
/// currently have no known leader. The generation-stamped result retains that
/// availability fact so routing can distinguish the leaderless selection.
pub fn select_java_keyed_topic_partition(
    serialized_key: &[u8],
    facts: TopicPartitionFacts<'_>,
) -> Result<PartitionSelection, KeyedPartitionError> {
    let partition = select_java_keyed_partition(serialized_key, facts.logical_count())?;
    facts
        .select(partition)
        .map_err(KeyedPartitionError::IncoherentTopicFacts)
}

pub(super) fn java_key_length(length: usize) -> Result<u32, KeyedPartitionError> {
    let length =
        u32::try_from(length).map_err(|_| KeyedPartitionError::KeyLengthUnrepresentable)?;
    if length > JAVA_SIGNED_INT_MAX {
        Err(KeyedPartitionError::KeyLengthUnrepresentable)
    } else {
        Ok(length)
    }
}

fn murmur2(serialized_key: &[u8], key_length: u32) -> u32 {
    let mut hash = MURMUR2_SEED ^ key_length;
    let mut chunks = serialized_key.chunks_exact(4);
    for chunk in &mut chunks {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(chunk);
        let mut word = u32::from_le_bytes(bytes);
        word = word.wrapping_mul(MURMUR2_MULTIPLIER);
        word ^= word >> 24;
        word = word.wrapping_mul(MURMUR2_MULTIPLIER);
        hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
        hash ^= word;
    }

    let tail = chunks.remainder();
    let mut tail_word = 0_u32;
    let mut shift = 0_u32;
    for byte in tail {
        tail_word |= u32::from(*byte) << shift;
        shift += 8;
    }
    if !tail.is_empty() {
        hash ^= tail_word;
        hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
    }

    hash ^= hash >> 13;
    hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
    hash ^ (hash >> 15)
}
