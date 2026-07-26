//! Unique initialized idle transactional producer ownership.

use crate::bridge::transaction::TransactionalProducerEngine;

use super::TransactionalProducerIdentity;

/// Uniquely controlled initialized transactional producer.
///
/// This first vertical slice exposes identity and lifecycle only. Beginning,
/// sending, committing, and aborting transactions remain unavailable until
/// their deterministic policy and execution owners land.
#[must_use = "close or retain the transactional producer owner"]
pub struct TransactionalProducer {
    engine: TransactionalProducerEngine,
}

impl TransactionalProducer {
    pub(crate) const fn from_bridge(engine: TransactionalProducerEngine) -> Self {
        Self { engine }
    }

    /// Returns the exact configured transactional ID.
    pub fn transactional_id(&self) -> &str {
        self.engine.transactional_id()
    }

    /// Returns Kafka's stable broker-issued producer identity.
    pub fn identity(&self) -> TransactionalProducerIdentity {
        TransactionalProducerIdentity::new(self.engine.producer_id(), self.engine.producer_epoch())
    }

    /// Returns whether the engine still recognizes this owner as active.
    pub fn is_active(&self) -> bool {
        self.engine.is_active()
    }

    /// Explicitly fences and releases this idle transactional owner.
    pub fn close(self) {
        self.engine.close();
    }
}

impl std::fmt::Debug for TransactionalProducer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionalProducer")
            .field("transactional_id", &self.transactional_id())
            .field("identity", &self.identity())
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}
