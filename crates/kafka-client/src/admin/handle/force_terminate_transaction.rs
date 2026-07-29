//! Single-transaction termination entry point over producer fencing.

use std::{iter, time::Instant};

use super::Admin;
use crate::admin::ForceTerminateTransactionBuilder;

impl Admin {
    /// Builds inert intent to force-terminate one ongoing transaction.
    pub fn force_terminate_transaction<S>(
        &self,
        transactional_id: S,
    ) -> ForceTerminateTransactionBuilder
    where
        S: Into<String>,
    {
        let boundary = Instant::now();
        ForceTerminateTransactionBuilder::from_fence_producers(
            self.fence_producers_from_boundary(iter::once(transactional_id), boundary),
        )
    }
}
