//! Cluster-wide transaction-listing entry point on the public admin handle.

use super::Admin;
use crate::admin::ListTransactionsBuilder;

impl Admin {
    /// Builds an inert cluster-wide transaction-listing query.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`ListTransactionsBuilder::submit`] is called.
    pub fn list_transactions(&self) -> ListTransactionsBuilder {
        ListTransactionsBuilder::new(self.engine.clone(), self.engine.default_timeout())
    }
}
