//! Inert user intent for one transactional-owner initialization.

use std::time::Duration;

use crate::bridge::transaction::TransactionalProducerInitializer;

use super::InitializeTransactionalProducer;

/// Linear builder for one uniquely controlled transactional producer.
#[must_use = "call build to submit transactional producer initialization"]
pub struct TransactionalProducerBuilder {
    initializer: TransactionalProducerInitializer,
    transactional_id: String,
    transaction_timeout: Option<Duration>,
    initialization_timeout: Option<Duration>,
}

impl TransactionalProducerBuilder {
    pub(crate) const fn new(
        initializer: TransactionalProducerInitializer,
        transactional_id: String,
    ) -> Self {
        Self {
            initializer,
            transactional_id,
            transaction_timeout: None,
            initialization_timeout: None,
        }
    }

    /// Overrides Kafka's broker-side transaction timeout for this owner.
    pub const fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_timeout = Some(timeout);
        self
    }

    /// Overrides the end-to-end initialization deadline.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.initialization_timeout = Some(timeout);
        self
    }

    /// Captures one absolute deadline and attempts bounded initialization.
    pub fn build(self) -> InitializeTransactionalProducer {
        InitializeTransactionalProducer::from_bridge(self.initializer.initialize(
            self.transactional_id,
            self.transaction_timeout,
            self.initialization_timeout,
        ))
    }
}

impl std::fmt::Debug for TransactionalProducerBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionalProducerBuilder")
            .field("transactional_id", &self.transactional_id)
            .field("transaction_timeout", &self.transaction_timeout)
            .field("initialization_timeout", &self.initialization_timeout)
            .finish_non_exhaustive()
    }
}
