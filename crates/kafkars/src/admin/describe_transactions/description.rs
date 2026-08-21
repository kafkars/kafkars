//! Stable successful Admin `DescribeTransactions` facts.

use super::TransactionTopic;

/// Kafka's bounded API 65 description of one transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionDescription {
    transaction_state: String,
    transaction_timeout_ms: i32,
    transaction_start_time_ms: Option<i64>,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<TransactionTopic>,
}

impl TransactionDescription {
    pub(crate) const fn new(
        transaction_state: String,
        transaction_timeout_ms: i32,
        transaction_start_time_ms: Option<i64>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<TransactionTopic>,
    ) -> Self {
        Self {
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
        }
    }

    /// Returns Kafka's exact transaction-state spelling.
    pub fn transaction_state(&self) -> &str {
        &self.transaction_state
    }

    /// Returns Kafka's exact signed transaction timeout in milliseconds.
    pub const fn transaction_timeout_ms(&self) -> i32 {
        self.transaction_timeout_ms
    }

    /// Returns the nonnegative transaction start time, if Kafka represented one.
    pub const fn transaction_start_time_ms(&self) -> Option<i64> {
        self.transaction_start_time_ms
    }

    /// Returns Kafka's exact signed producer identity.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's exact signed producer epoch.
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns participating topics in deterministic UTF-8 byte order.
    pub fn topics(&self) -> &[TransactionTopic] {
        &self.topics
    }
}
