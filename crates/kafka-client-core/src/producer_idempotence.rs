//! Core-owned nontransactional producer identity and sequence value types.

use core::num::NonZeroU32;

/// Generation fencing one lazy nontransactional identity acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProducerIdentityGeneration(NonZeroU32);

impl ProducerIdentityGeneration {
    /// First acquisition generation.
    pub const fn initial() -> Self {
        Self(NonZeroU32::MIN)
    }

    /// Returns the stable nonzero value.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Broker-issued nontransactional producer identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl ProducerIdentity {
    /// Validates one broker-issued identity.
    pub const fn try_new(producer_id: i64, producer_epoch: i16) -> Option<Self> {
        if producer_id < 0 || producer_epoch < 0 {
            None
        } else {
            Some(Self {
                producer_id,
                producer_epoch,
            })
        }
    }

    /// Returns Kafka's broker-issued producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's broker-issued producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}

/// One partition-local contiguous sequence range assigned before encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSequenceLease {
    base_sequence: i32,
    record_count: u32,
}

impl ProducerSequenceLease {
    /// Validates one nonempty Kafka sequence range.
    pub const fn try_new(base_sequence: i32, record_count: u32) -> Option<Self> {
        if base_sequence < 0 || record_count == 0 {
            None
        } else {
            Some(Self {
                base_sequence,
                record_count,
            })
        }
    }

    pub(crate) const fn with_record_count(self, record_count: u32) -> Option<Self> {
        Self::try_new(self.base_sequence, record_count)
    }

    /// Returns the first Kafka sequence encoded into the batch.
    pub const fn base_sequence(self) -> i32 {
        self.base_sequence
    }

    /// Returns the exact number of records covered by this lease.
    pub const fn record_count(self) -> u32 {
        self.record_count
    }
}
