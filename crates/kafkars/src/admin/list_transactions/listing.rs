//! Stable wire-free public transaction listings and broker errors.

/// One transaction visible through a broker's transaction coordinator state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionListing {
    transactional_id: String,
    producer_id: i64,
    transaction_state: String,
}

impl TransactionListing {
    pub(crate) const fn new(
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

    /// Returns the stable transactional identifier.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns Kafka's exact signed producer identifier.
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the broker-owned transaction state without a local enum.
    pub fn transaction_state(&self) -> &str {
        &self.transaction_state
    }
}

/// Exact top-level `ListTransactions` rejection from one broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListTransactionsBrokerError {
    broker_id: i32,
    code: i16,
}

impl ListTransactionsBrokerError {
    pub(crate) const fn new(broker_id: i32, code: i16) -> Self {
        Self { broker_id, code }
    }

    /// Returns the exact broker identity.
    pub const fn broker_id(self) -> i32 {
        self.broker_id
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}
