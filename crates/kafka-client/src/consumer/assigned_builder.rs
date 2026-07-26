//! Inert construction of the engine's unique assigned-consumer capability.

use crate::bridge::ClientEngine;

use super::{AssignedConsumer, AssignedConsumerBuildError};

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
    ///
    /// Rejection returns this exact builder because no unique engine capability
    /// transferred to the call.
    pub fn build(self) -> Result<AssignedConsumer, AssignedConsumerBuildError> {
        match self.engine.claim_assigned_consumer() {
            Ok(engine) => Ok(AssignedConsumer::new(engine)),
            Err(error) => Err(AssignedConsumerBuildError::new(self, error)),
        }
    }
}
