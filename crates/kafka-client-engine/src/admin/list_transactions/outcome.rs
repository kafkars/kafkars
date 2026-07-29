//! Stable engine terminal values for Admin `ListTransactions`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsDeliveryStatus {
    /// No discovery or broker request reached the driver.
    NotSent,
    /// At least one request may have reached Kafka.
    PossiblySent,
}

/// One transaction reported by its current coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListedTransaction {
    transactional_id: String,
    producer_id: i64,
    transaction_state: String,
}

impl AdminListedTransaction {
    /// Consumes this listing into stable scalar parts.
    pub fn into_parts(self) -> (String, i64, String) {
        (
            self.transactional_id,
            self.producer_id,
            self.transaction_state,
        )
    }
}

/// Exact top-level API-key 66 error from one broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsBrokerError {
    broker_id: i32,
    code: i16,
}

impl AdminListTransactionsBrokerError {
    /// Consumes this error into exact broker and signed-code parts.
    pub const fn into_parts(self) -> (i32, i16) {
        (self.broker_id, self.code)
    }
}

/// Exact top-level discovery error with bounded diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsDiscoveryError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl AdminListTransactionsDiscoveryError {
    /// Consumes this error into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Successful cluster-wide terminal in deterministic byte order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsEngineBatch {
    throttle_time_ms: u32,
    unknown_state_filters: Vec<String>,
    transactions: Vec<AdminListedTransaction>,
    broker_errors: Vec<AdminListTransactionsBrokerError>,
}

impl AdminListTransactionsEngineBatch {
    /// Consumes maximum throttle, unknown filters, listings, and exact errors.
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

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver or local request admission rejected the current call.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Valid facts exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The requested filters cannot be represented by the broker version.
    Compatibility,
    /// Discovery, response, or cross-broker facts conflicted.
    InvalidResponse,
}

/// Whole-operation failure with cumulative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminListTransactionsFailure {
    kind: AdminListTransactionsFailureKind,
    delivery: AdminListTransactionsDeliveryStatus,
}

impl AdminListTransactionsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminListTransactionsFailureKind {
        self.kind
    }

    /// Returns cumulative delivery certainty.
    pub const fn delivery(self) -> AdminListTransactionsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsOutcome {
    /// Discovery and every exact-broker request settled.
    Listed(AdminListTransactionsEngineBatch),
    /// Controller-routed discovery returned an exact broker error.
    DiscoveryRejected(AdminListTransactionsDiscoveryError),
    /// A mechanism or structural failure stopped the operation.
    Failed(AdminListTransactionsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminListTransactionsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin ListTransactions result was already observed",
            Self::Stale => "Admin ListTransactions observer is stale",
        })
    }
}

impl std::error::Error for AdminListTransactionsObserverError {}
