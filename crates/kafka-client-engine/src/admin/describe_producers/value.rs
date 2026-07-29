//! Stable generated-free active-producer facts for Admin `DescribeProducers`.

/// One normalized active-producer state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducerState {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_transaction_start_offset: Option<i64>,
}

impl AdminDescribeProducerState {
    pub(super) const fn new(
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

    /// Consumes the state into stable scalar parts.
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
}
