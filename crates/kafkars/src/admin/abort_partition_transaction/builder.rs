//! Inert partition-transaction abort intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    abort_partition_transaction::AbortPartitionTransactionAdminRequest, admin::AdminEngine,
};

use super::{AbortPartitionTransaction, AbortTransactionSpec};

/// Inert request to abort one transaction on one Kafka partition.
#[must_use = "call submit to admit the AbortPartitionTransaction operation"]
pub struct AbortTransactionBuilder {
    engine: AdminEngine,
    request: AbortPartitionTransactionAdminRequest,
    timeout: Duration,
}

impl AbortTransactionBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        spec: AbortTransactionSpec,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request: AbortPartitionTransactionAdminRequest::new(spec),
            timeout,
        }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> AbortPartitionTransaction {
        AbortPartitionTransaction::from_bridge(
            self.engine
                .submit_abort_partition_transaction(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AbortTransactionBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AbortTransactionBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
