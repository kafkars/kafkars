//! Inert construction of the engine's unique assigned-consumer capability.

use crate::{KafkaError, bridge::ClientEngine};

use super::AssignedConsumer;

/// Builder for a consumer with direct partition ownership.
#[derive(Debug, Clone)]
pub struct AssignedConsumerBuilder {
    engine: ClientEngine,
}

impl AssignedConsumerBuilder {
    pub(crate) const fn new(engine: ClientEngine) -> Self {
        Self { engine }
    }

    /// Claims this client's sole directly assigned consumer.
    pub fn build(self) -> Result<AssignedConsumer, KafkaError> {
        self.engine
            .claim_assigned_consumer()
            .map(AssignedConsumer::new)
    }
}
