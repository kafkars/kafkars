//! Inert Admin `DescribeTransactions` intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, describe_transactions::DescribeTransactionsAdminRequest};

use super::DescribeTransactions;

/// Inert caller-ordered Admin `DescribeTransactions` request.
#[must_use = "call submit to admit the DescribeTransactions operation"]
pub struct DescribeTransactionsBuilder {
    engine: AdminEngine,
    request: DescribeTransactionsAdminRequest,
    timeout: Duration,
}

impl DescribeTransactionsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeTransactionsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This is the public operation boundary. The engine captures its absolute
    /// deadline before validation or admission.
    pub fn submit(self) -> DescribeTransactions {
        DescribeTransactions::from_bridge(
            self.engine
                .submit_describe_transactions(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeTransactionsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTransactionsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
