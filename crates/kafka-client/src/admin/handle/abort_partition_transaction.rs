//! Partition-transaction abort entry point on the shared public admin handle.

use super::Admin;
use crate::admin::{AbortTransactionBuilder, AbortTransactionSpec};

impl Admin {
    /// Builds inert intent to abort one exact partition transaction.
    ///
    /// No timeout starts and no destructive request is admitted until
    /// [`AbortTransactionBuilder::submit`] is called.
    pub fn abort_transaction(&self, spec: AbortTransactionSpec) -> AbortTransactionBuilder {
        AbortTransactionBuilder::new(self.engine.clone(), spec, self.engine.default_timeout())
    }
}
