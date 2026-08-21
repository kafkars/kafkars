//! Inert public `DescribeTransactions` intent translated at the engine boundary.

use super::engine::Request as EngineRequest;

/// Linear request retained by the public builder before submission.
pub(crate) struct DescribeTransactionsAdminRequest {
    transactional_ids: Vec<String>,
}

impl DescribeTransactionsAdminRequest {
    pub(crate) const fn new(transactional_ids: Vec<String>) -> Self {
        Self { transactional_ids }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.transactional_ids)
    }
}

impl std::fmt::Debug for DescribeTransactionsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTransactionsAdminRequest")
            .field("transactional_ids", &self.transactional_ids)
            .finish()
    }
}
