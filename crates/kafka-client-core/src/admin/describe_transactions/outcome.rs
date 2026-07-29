//! Caller-correlated transaction-description outcomes and terminal facts.

use core::num::NonZeroI16;

use super::{AdminDescribeTransactionDescription, AdminDescribeTransactionsFailure};

/// Exact per-transaction broker rejection.
///
/// API 65 v0 carries an error code but no broker diagnostic string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionBrokerError {
    code: NonZeroI16,
}

impl AdminDescribeTransactionBrokerError {
    /// Creates one exact signed Kafka error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Exact result Kafka returned for one requested transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionResult {
    /// Kafka returned one bounded transaction description.
    Described(AdminDescribeTransactionDescription),
    /// Kafka rejected this transactional ID with an exact signed code.
    BrokerFailed(AdminDescribeTransactionBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionOutcome {
    transactional_id: String,
    result: AdminDescribeTransactionResult,
}

impl AdminDescribeTransactionOutcome {
    /// Creates one successful transaction-description outcome.
    pub const fn described(
        transactional_id: String,
        description: AdminDescribeTransactionDescription,
    ) -> Self {
        Self {
            transactional_id,
            result: AdminDescribeTransactionResult::Described(description),
        }
    }

    /// Creates one exact per-ID broker rejection.
    pub const fn broker_failed(
        transactional_id: String,
        error: AdminDescribeTransactionBrokerError,
    ) -> Self {
        Self {
            transactional_id,
            result: AdminDescribeTransactionResult::BrokerFailed(error),
        }
    }

    /// Returns the correlated transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns this ID's exact result.
    pub const fn result(&self) -> &AdminDescribeTransactionResult {
        &self.result
    }

    /// Consumes this outcome into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, AdminDescribeTransactionResult) {
        (self.transactional_id, self.result)
    }

    pub(crate) fn description_mut(&mut self) -> Option<&mut AdminDescribeTransactionDescription> {
        match &mut self.result {
            AdminDescribeTransactionResult::Described(description) => Some(description),
            AdminDescribeTransactionResult::BrokerFailed(_) => None,
        }
    }
}

/// Caller-ordered result for every requested transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminDescribeTransactionOutcome>,
}

impl AdminDescribeTransactionsBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AdminDescribeTransactionOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-ID outcomes in exact caller order.
    pub fn outcomes(&self) -> &[AdminDescribeTransactionOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into stable adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AdminDescribeTransactionOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision for Admin `DescribeTransactions`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsTerminal {
    /// Every requested transactional ID settled in caller order.
    Described(AdminDescribeTransactionsBatch),
    /// A whole-operation mechanism failure occurred.
    Failed(AdminDescribeTransactionsFailure),
}
