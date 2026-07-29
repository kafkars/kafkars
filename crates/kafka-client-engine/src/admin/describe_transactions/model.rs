//! Engine-owned scalar intent for one Admin `DescribeTransactions` query.

use kafka_client_core::{AdminDescribeTransactionsPlan, AdminDescribeTransactionsPlanError};

/// One caller-ordered bounded transaction-description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeTransactionsRequest {
    transactional_ids: Vec<String>,
}

impl AdminDescribeTransactionsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(transactional_ids: Vec<String>) -> Self {
        Self { transactional_ids }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.transactional_ids = self
            .transactional_ids
            .into_iter()
            .map(|value| value.into_boxed_str().into_string())
            .collect();
        self.transactional_ids.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AdminDescribeTransactionsPlan, AdminDescribeTransactionsPlanError> {
        AdminDescribeTransactionsPlan::new(self.transactional_ids)
    }
}
