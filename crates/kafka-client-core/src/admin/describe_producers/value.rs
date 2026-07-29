//! Generated-type-free active-producer facts returned by Kafka.

/// Stable active-producer state for one topic-partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminProducerState {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_transaction_start_offset: Option<i64>,
}

impl AdminProducerState {
    /// Creates one protocol-normalized active-producer state.
    pub const fn new(
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

    /// Returns Kafka's producer identity.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's producer epoch.
    pub const fn producer_epoch(self) -> i32 {
        self.producer_epoch
    }

    /// Returns the last sequence, including Kafka's `-1` initial sentinel.
    pub const fn last_sequence(self) -> i32 {
        self.last_sequence
    }

    /// Returns the last timestamp, including Kafka's `-1` initial sentinel.
    pub const fn last_timestamp(self) -> i64 {
        self.last_timestamp
    }

    /// Returns Kafka's producer-group coordinator epoch.
    pub const fn coordinator_epoch(self) -> i32 {
        self.coordinator_epoch
    }

    /// Returns the current transaction's start offset, when active.
    pub const fn current_transaction_start_offset(self) -> Option<i64> {
        self.current_transaction_start_offset
    }

    /// Consumes the fact into stable scalar parts.
    pub const fn into_parts(self) -> (i64, i32, i32, i64, i32, Option<i64>) {
        (
            self.producer_id,
            self.producer_epoch,
            self.last_sequence,
            self.last_timestamp,
            self.coordinator_epoch,
            self.current_transaction_start_offset,
        )
    }

    pub(crate) const fn is_well_formed(self) -> bool {
        self.producer_id >= 0
            && self.producer_epoch >= 0
            && self.last_sequence >= -1
            && self.last_timestamp >= -1
            && self.coordinator_epoch >= 0
            && match self.current_transaction_start_offset {
                Some(offset) => offset >= 0,
                None => true,
            }
    }
}
