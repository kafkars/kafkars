//! Private engine boundary for unique transactional-owner initialization.

use std::time::Duration;

use crate::{
    Engine,
    transaction::{TransactionInitializationCapture, TransactionInitializationCaptureError},
};

impl Engine {
    /// Captures one absolute initialization deadline before request conversion.
    pub fn capture_transactional_owner_initialization(
        &self,
        operation_timeout: Duration,
    ) -> Result<TransactionInitializationCapture, TransactionInitializationCaptureError> {
        let lifetime: std::sync::Arc<dyn Send + Sync> = self.inner.clone();
        self.inner
            .transaction_initialization
            .capture(operation_timeout, lifetime)
    }
}
