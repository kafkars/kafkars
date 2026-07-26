//! Private engine boundary for unique transactional-owner initialization.

use std::time::Duration;

use crate::{
    Engine,
    transaction::{
        TransactionInitializationAccepted, TransactionInitializationAdmissionError,
        TransactionInitializationRequest,
    },
};

impl Engine {
    pub(crate) fn try_initialize_transactional_owner(
        &self,
        request: TransactionInitializationRequest,
        operation_timeout: Duration,
    ) -> Result<TransactionInitializationAccepted, TransactionInitializationAdmissionError> {
        let lifetime: std::sync::Arc<dyn Send + Sync> = self.inner.clone();
        self.inner
            .transaction_initialization
            .try_initialize(request, operation_timeout, lifetime)
    }
}
