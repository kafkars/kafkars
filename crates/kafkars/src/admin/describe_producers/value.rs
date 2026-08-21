//! Stable successful Admin `DescribeProducers` producer facts.

/// One normalized active producer state for a Kafka partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerState {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_transaction_start_offset: Option<i64>,
}

impl ProducerState {
    pub(crate) const fn new(
        producer_id: i64,
        producer_epoch: i32,
        last_sequence: i32,
        last_timestamp: i64,
        coordinator_epoch: i32,
        current_transaction_start_offset: Option<i64>,
    ) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_transaction_start_offset,
        }
    }

    /// Returns Kafka's exact nonnegative producer identity.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's exact nonnegative producer epoch.
    pub const fn producer_epoch(&self) -> i32 {
        self.producer_epoch
    }

    /// Returns the exact last sequence, retaining Kafka's `-1` sentinel.
    pub const fn last_sequence(&self) -> i32 {
        self.last_sequence
    }

    /// Returns the exact last timestamp, retaining Kafka's `-1` sentinel.
    pub const fn last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    /// Returns Kafka's exact nonnegative transaction coordinator epoch.
    pub const fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }

    /// Returns the current transaction's nonnegative first offset, if active.
    pub const fn current_transaction_start_offset(&self) -> Option<i64> {
        self.current_transaction_start_offset
    }
}
