//! Successful cluster-wide transaction listing result.

use std::time::Duration;

use super::{ListTransactionsBrokerError, TransactionListing};

/// Fully settled cluster-wide listing with partial broker errors preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListTransactionsResult {
    throttle_time: Duration,
    transactions: Vec<TransactionListing>,
    unknown_state_filters: Vec<String>,
    broker_errors: Vec<ListTransactionsBrokerError>,
}

impl ListTransactionsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        transactions: Vec<TransactionListing>,
        unknown_state_filters: Vec<String>,
        broker_errors: Vec<ListTransactionsBrokerError>,
    ) -> Self {
        Self {
            throttle_time,
            transactions,
            unknown_state_filters,
            broker_errors,
        }
    }

    /// Returns the maximum throttle observed across exact-broker calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns globally transactional-ID byte-sorted listings.
    pub fn transactions(&self) -> &[TransactionListing] {
        &self.transactions
    }

    /// Returns canonical byte-sorted state filters unknown to the brokers.
    pub fn unknown_state_filters(&self) -> &[String] {
        &self.unknown_state_filters
    }

    /// Returns exact broker-scoped errors ordered by broker ID.
    pub fn broker_errors(&self) -> &[ListTransactionsBrokerError] {
        &self.broker_errors
    }

    /// Consumes this result into stable listing, unknown-filter, and error parts.
    pub fn into_parts(
        self,
    ) -> (
        Duration,
        Vec<TransactionListing>,
        Vec<String>,
        Vec<ListTransactionsBrokerError>,
    ) {
        (
            self.throttle_time,
            self.transactions,
            self.unknown_state_filters,
            self.broker_errors,
        )
    }
}
