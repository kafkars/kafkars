//! Transaction-description entry point on the shared public admin handle.

use super::Admin;
use crate::{
    admin::DescribeTransactionsBuilder,
    bridge::describe_transactions::DescribeTransactionsAdminRequest,
};

impl Admin {
    /// Builds an inert caller-ordered transaction-description query.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`DescribeTransactionsBuilder::submit`] is called.
    pub fn describe_transactions<I, S>(&self, transactional_ids: I) -> DescribeTransactionsBuilder
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        DescribeTransactionsBuilder::new(
            self.engine.clone(),
            DescribeTransactionsAdminRequest::new(
                transactional_ids.into_iter().map(Into::into).collect(),
            ),
            self.engine.default_timeout(),
        )
    }
}
