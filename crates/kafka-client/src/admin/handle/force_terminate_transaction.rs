//! Single-transaction termination entry point over producer fencing.

use std::iter;

use super::Admin;
use crate::admin::ForceTerminateTransactionBuilder;

impl Admin {
    /// Builds inert intent to force-terminate one ongoing transaction.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`ForceTerminateTransactionBuilder::submit`] is called.
    pub fn force_terminate_transaction<S>(
        &self,
        transactional_id: S,
    ) -> ForceTerminateTransactionBuilder
    where
        S: Into<String>,
    {
        ForceTerminateTransactionBuilder::from_fence_producers(
            self.fence_producers(iter::once(transactional_id)),
        )
    }
}
