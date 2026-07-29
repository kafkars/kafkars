//! Generated-type-free transaction-listing facts returned by Kafka.

/// Maximum discovered brokers retained by one cluster-wide listing.
pub const LIST_TRANSACTIONS_MAX_BROKERS: usize = 4 * 1024;
/// Maximum unknown state filters retained across one complete operation.
pub const LIST_TRANSACTIONS_MAX_UNKNOWN_STATE_FILTERS: usize = 4 * 1024;
/// Maximum transaction facts retained across one complete operation.
pub const LIST_TRANSACTIONS_MAX_TRANSACTIONS: usize = 32 * 1024;
/// Maximum bytes retained for one broker-reported transactional ID.
pub const LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES: usize = i16::MAX as usize;
/// Maximum bytes retained for one broker-reported transaction-state spelling.
pub const LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES: usize = 1024;
/// Maximum aggregate string bytes retained across one complete operation.
pub const LIST_TRANSACTIONS_MAX_RESULT_STRING_BYTES: usize = 1024 * 1024;

/// One transaction reported by one broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListedTransaction {
    transactional_id: String,
    producer_id: i64,
    transaction_state: String,
}

impl AdminListedTransaction {
    /// Creates one protocol-normalized transaction fact.
    pub const fn new(
        transactional_id: String,
        producer_id: i64,
        transaction_state: String,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            transaction_state,
        }
    }

    /// Returns Kafka's exact transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns Kafka's exact signed producer ID.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's exact transaction-state spelling.
    pub fn transaction_state(&self) -> &str {
        &self.transaction_state
    }

    /// Consumes this fact into adapter-owned parts.
    pub fn into_parts(self) -> (String, i64, String) {
        (
            self.transactional_id,
            self.producer_id,
            self.transaction_state,
        )
    }
}
