//! Broker-correlated outcomes and terminal transaction-listing facts.

use core::num::NonZeroI16;

use super::{
    super::DescribeClusterBrokerError, AdminListTransactionsFailure, AdminListedTransaction,
};

/// Exact top-level `ListTransactions` rejection from one broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsBrokerError {
    broker_id: i32,
    code: NonZeroI16,
}

impl AdminListTransactionsBrokerError {
    /// Creates one exact broker-scoped signed Kafka error.
    pub const fn new(broker_id: i32, code: NonZeroI16) -> Self {
        Self { broker_id, code }
    }

    /// Returns the exact broker identity.
    pub const fn broker_id(self) -> i32 {
        self.broker_id
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes this error into adapter-owned parts.
    pub const fn into_parts(self) -> (i32, i16) {
        (self.broker_id, self.code.get())
    }
}

/// One correlated, structurally valid exact-broker response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsBrokerOutcome {
    /// Kafka returned local transaction and unknown-filter facts.
    Listed {
        /// Exact broker identity used for routing.
        broker_id: i32,
        /// State filters this broker did not recognize.
        unknown_state_filters: Vec<String>,
        /// Transactions coordinated by this broker.
        transactions: Vec<AdminListedTransaction>,
    },
    /// Kafka returned a nonzero top-level error.
    Rejected(AdminListTransactionsBrokerError),
}

impl AdminListTransactionsBrokerOutcome {
    /// Returns the correlated broker identity.
    pub const fn broker_id(&self) -> i32 {
        match self {
            Self::Listed { broker_id, .. } => *broker_id,
            Self::Rejected(error) => error.broker_id(),
        }
    }
}

/// Successful cluster-wide terminal in deterministic byte order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsBatch {
    throttle_time_ms: u32,
    unknown_state_filters: Vec<String>,
    transactions: Vec<AdminListedTransaction>,
    broker_errors: Vec<AdminListTransactionsBrokerError>,
}

impl AdminListTransactionsBatch {
    /// Creates one fully settled cluster-wide listing.
    pub const fn new(
        throttle_time_ms: u32,
        unknown_state_filters: Vec<String>,
        transactions: Vec<AdminListedTransaction>,
        broker_errors: Vec<AdminListTransactionsBrokerError>,
    ) -> Self {
        Self {
            throttle_time_ms,
            unknown_state_filters,
            transactions,
            broker_errors,
        }
    }

    /// Returns the maximum nonnegative throttle observed across broker calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns deduplicated unknown filters in strict UTF-8 byte order.
    pub fn unknown_state_filters(&self) -> &[String] {
        &self.unknown_state_filters
    }

    /// Returns deduplicated transactions in strict transactional-ID byte order.
    pub fn transactions(&self) -> &[AdminListedTransaction] {
        &self.transactions
    }

    /// Returns exact top-level broker errors in broker-ID order.
    pub fn broker_errors(&self) -> &[AdminListTransactionsBrokerError] {
        &self.broker_errors
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(
        self,
    ) -> (
        u32,
        Vec<String>,
        Vec<AdminListedTransaction>,
        Vec<AdminListTransactionsBrokerError>,
    ) {
        (
            self.throttle_time_ms,
            self.unknown_state_filters,
            self.transactions,
            self.broker_errors,
        )
    }
}

/// Exactly one terminal decision for Admin `ListTransactions`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsTerminal {
    /// Discovery and every exact-broker request settled.
    Listed(AdminListTransactionsBatch),
    /// Controller-routed discovery returned an exact broker rejection.
    DiscoveryRejected(DescribeClusterBrokerError),
    /// A whole-operation mechanism or structural failure occurred.
    Failed(AdminListTransactionsFailure),
}
