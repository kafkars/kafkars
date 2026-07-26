//! Linear public-boundary deadline capture for transaction initialization.

use std::sync::Arc;

use crate::clock::DeadlineCapture;

use super::{
    TransactionInitializationAccepted, TransactionInitializationAdmissionError,
    TransactionInitializationRequest, shard::TransactionInitializationShardState,
};

/// One absolute initialization deadline captured before request conversion.
#[must_use = "consume the capture to initialize one transactional owner"]
pub struct TransactionInitializationCapture {
    shared: Arc<TransactionInitializationShardState>,
    deadline: DeadlineCapture,
    lifetime: Arc<dyn Send + Sync>,
}

impl TransactionInitializationCapture {
    pub(super) const fn new(
        shared: Arc<TransactionInitializationShardState>,
        deadline: DeadlineCapture,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            shared,
            deadline,
            lifetime,
        }
    }

    /// Consumes this exact deadline while attempting bounded initialization.
    pub fn initialize_transactional_owner(
        self,
        request: TransactionInitializationRequest,
    ) -> Result<TransactionInitializationAccepted, TransactionInitializationAdmissionError> {
        super::port::try_initialize_captured(&self.shared, self.deadline, request, self.lifetime)
    }
}

impl std::fmt::Debug for TransactionInitializationCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionInitializationCapture")
            .finish_non_exhaustive()
    }
}
