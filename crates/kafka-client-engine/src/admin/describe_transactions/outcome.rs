//! Stable engine terminal values for Admin `DescribeTransactions`.

use core::fmt;

use super::AdminDescribeTransactionDescription;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsDeliveryStatus {
    /// No transactional-ID call in the operation reached Kafka.
    NotSent,
    /// At least one transactional-ID call may have reached Kafka.
    PossiblySent,
}

/// Exact transactional-ID-level broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionEngineBrokerError {
    code: i16,
}

impl AdminDescribeTransactionEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }

    /// Consumes the error into its exact signed code.
    pub const fn into_parts(self) -> i16 {
        self.code
    }
}

/// One caller-correlated transaction-description result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionEngineResult {
    transactional_id: String,
    result: Result<AdminDescribeTransactionDescription, AdminDescribeTransactionEngineBrokerError>,
}

impl AdminDescribeTransactionEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Result<AdminDescribeTransactionDescription, AdminDescribeTransactionEngineBrokerError>,
    ) {
        (self.transactional_id, self.result)
    }
}

/// Caller-ordered complete result plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsEngineBatch {
    throttle_time_ms: u32,
    results: Vec<AdminDescribeTransactionEngineResult>,
}

impl AdminDescribeTransactionsEngineBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AdminDescribeTransactionEngineResult>) {
        (self.throttle_time_ms, self.results)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the current transactional ID.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the operation.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsFailure {
    kind: AdminDescribeTransactionsFailureKind,
    delivery: AdminDescribeTransactionsDeliveryStatus,
}

impl AdminDescribeTransactionsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeTransactionsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AdminDescribeTransactionsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsOutcome {
    /// Every requested transactional ID settled in caller order.
    Described(AdminDescribeTransactionsEngineBatch),
    /// Execution failed outside an exact transactional-ID broker result.
    Failed(AdminDescribeTransactionsFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminDescribeTransactionsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DescribeTransactions result was already observed",
            Self::Stale => "Admin DescribeTransactions observer is stale",
        })
    }
}

impl std::error::Error for AdminDescribeTransactionsObserverError {}
