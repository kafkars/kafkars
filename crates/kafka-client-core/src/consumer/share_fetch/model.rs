//! Validated, bytes-free `ShareFetch` acquisition-range facts.

use crate::{AssignedTopicPartition, ByteCount, Deadline, Moment};

/// Nonzero Kafka topic UUID retained independently of the local topic catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareTopicUuid([u8; 16]);

impl ShareTopicUuid {
    /// Accepts one nonzero Kafka topic UUID.
    pub const fn try_from_bytes(value: [u8; 16]) -> Option<Self> {
        let mut index = 0;
        while index < value.len() {
            if value[index] != 0 {
                return Some(Self(value));
            }
            index += 1;
        }
        None
    }

    /// Returns the exact Kafka UUID bytes.
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Positive delivery count issued with one acquired range.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ShareDeliveryCount(i16);

impl ShareDeliveryCount {
    /// Accepts Kafka's positive delivery-count domain.
    pub const fn try_from_raw(value: i16) -> Option<Self> {
        if value <= 0 { None } else { Some(Self(value)) }
    }

    /// Returns the Kafka delivery count.
    pub const fn get(self) -> i16 {
        self.0
    }
}

/// Inclusive nonnegative offsets for one acquired range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareAcquiredOffsets {
    first: i64,
    last: i64,
}

impl ShareAcquiredOffsets {
    /// Validates nonnegative ordered inclusive offsets.
    pub const fn try_new(first: i64, last: i64) -> Result<Self, ShareAcquiredRangeError> {
        if first < 0 || last < first {
            Err(ShareAcquiredRangeError::InvalidOffsets)
        } else {
            Ok(Self { first, last })
        }
    }

    /// Returns the inclusive first offset.
    pub const fn first(self) -> i64 {
        self.first
    }

    /// Returns the inclusive last offset.
    pub const fn last(self) -> i64 {
        self.last
    }

    /// Returns the number of offsets represented by this range.
    pub const fn count(self) -> u64 {
        self.last.unsigned_abs() - self.first.unsigned_abs() + 1
    }
}

/// One validated acquired offset range decoded from `ShareFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareAcquiredRange {
    topic_uuid: ShareTopicUuid,
    partition: AssignedTopicPartition,
    offsets: ShareAcquiredOffsets,
    delivery_count: ShareDeliveryCount,
    retained_bytes: ByteCount,
    lock_deadline: Deadline,
}

impl ShareAcquiredRange {
    /// Validates ordered nonnegative offsets and a future lock boundary.
    pub const fn try_new(
        topic_uuid: ShareTopicUuid,
        partition: AssignedTopicPartition,
        offsets: ShareAcquiredOffsets,
        delivery_count: ShareDeliveryCount,
        retained_bytes: ByteCount,
        lock_deadline: Deadline,
        now: Moment,
    ) -> Result<Self, ShareAcquiredRangeError> {
        if lock_deadline.is_elapsed_at(now) {
            return Err(ShareAcquiredRangeError::ExpiredLock);
        }
        Ok(Self {
            topic_uuid,
            partition,
            offsets,
            delivery_count,
            retained_bytes,
            lock_deadline,
        })
    }

    /// Returns the exact Kafka topic UUID.
    pub const fn topic_uuid(self) -> ShareTopicUuid {
        self.topic_uuid
    }

    /// Returns the engine-catalog topic and partition.
    pub const fn partition(self) -> AssignedTopicPartition {
        self.partition
    }

    /// Returns the inclusive first offset.
    pub const fn first_offset(self) -> i64 {
        self.offsets.first()
    }

    /// Returns the inclusive last offset.
    pub const fn last_offset(self) -> i64 {
        self.offsets.last()
    }

    /// Returns the number of offsets represented by this range.
    pub const fn record_count(self) -> u64 {
        self.offsets.count()
    }

    /// Returns the broker-issued delivery count.
    pub const fn delivery_count(self) -> ShareDeliveryCount {
        self.delivery_count
    }

    /// Returns the engine-owned byte charge.
    pub const fn retained_bytes(self) -> ByteCount {
        self.retained_bytes
    }

    /// Returns the conservative local lock boundary.
    pub const fn lock_deadline(self) -> Deadline {
        self.lock_deadline
    }

    pub(super) fn overlaps(self, other: Self) -> bool {
        self.topic_uuid == other.topic_uuid
            && self.partition.partition() == other.partition.partition()
            && self.first_offset() <= other.last_offset()
            && other.first_offset() <= self.last_offset()
    }

    pub(super) fn conflicts_topic_identity(self, other: Self) -> bool {
        (self.topic_uuid == other.topic_uuid
            && self.partition.topic_id() != other.partition.topic_id())
            || (self.topic_uuid != other.topic_uuid
                && self.partition.topic_id() == other.partition.topic_id())
    }
}

/// Structural rejection while validating one acquired range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcquiredRangeError {
    /// Offsets were negative, reversed, or otherwise unrepresentable.
    InvalidOffsets,
    /// The conservative lock boundary was already elapsed.
    ExpiredLock,
}
