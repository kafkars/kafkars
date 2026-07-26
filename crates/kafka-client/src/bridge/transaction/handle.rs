//! Capture-first engine submission for one transactional owner.

use std::time::Duration;

use kafka_client_engine::{Engine, TransactionInitializationRequest};

use super::{TransactionInitialization, result::translate_capture_error};

/// Private bridge retaining engine defaults and initialization capability.
pub(crate) struct TransactionalProducerInitializer {
    engine: Engine,
}

impl TransactionalProducerInitializer {
    pub(crate) const fn new(engine: Engine) -> Self {
        Self { engine }
    }

    pub(crate) fn initialize(
        self,
        transactional_id: String,
        transaction_timeout: Option<Duration>,
        initialization_timeout: Option<Duration>,
    ) -> TransactionInitialization {
        let operation_timeout = initialization_timeout
            .unwrap_or_else(|| self.engine.config().transaction_initialization_timeout());
        let capture = match self
            .engine
            .capture_transactional_owner_initialization(operation_timeout)
        {
            Ok(capture) => capture,
            Err(error) => {
                return TransactionInitialization::ready(Err(translate_capture_error(error)));
            }
        };
        let broker_timeout =
            transaction_timeout.unwrap_or_else(|| self.engine.config().transaction_timeout());
        let request = TransactionInitializationRequest::new(
            transactional_id,
            transaction_timeout_ms(broker_timeout),
        );
        TransactionInitialization::from_admission(capture.initialize_transactional_owner(request))
    }
}

impl std::fmt::Debug for TransactionalProducerInitializer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionalProducerInitializer")
            .finish_non_exhaustive()
    }
}

pub(super) fn transaction_timeout_ms(timeout: Duration) -> u32 {
    u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX)
}
