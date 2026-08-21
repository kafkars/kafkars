//! Public caller-ordered Admin `DescribeTransactions` result.

use std::time::Duration;

use super::{super::BatchResult, TransactionDescription};

/// Completed transaction descriptions with maximum throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTransactionsResult {
    throttle_time: Duration,
    transactions: BatchResult<String, TransactionDescription>,
}

impl DescribeTransactionsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        transactions: BatchResult<String, TransactionDescription>,
    ) -> Self {
        Self {
            throttle_time,
            transactions,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-ID outcomes in original caller order.
    pub const fn transactions(&self) -> &BatchResult<String, TransactionDescription> {
        &self.transactions
    }

    /// Consumes this result into caller-ordered per-ID outcomes.
    pub fn into_transactions(self) -> BatchResult<String, TransactionDescription> {
        self.transactions
    }
}
