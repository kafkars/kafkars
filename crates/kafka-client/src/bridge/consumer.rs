//! Unique facade ownership over the engine's assigned-consumer capability.

use kafka_client_engine::{
    AssignedConsumerHandle as EngineAssignedConsumerHandle, Engine as SharedEngine,
};

use crate::KafkaError;

use super::{
    consumer_close::AssignedConsumerClose, consumer_result::translate_assigned_consumer_claim,
};

/// Private linear bridge retaining the engine's sole assigned-consumer handle.
pub(crate) struct AssignedConsumerEngine {
    handle: EngineAssignedConsumerHandle,
}

impl AssignedConsumerEngine {
    pub(crate) fn claim(engine: &SharedEngine) -> Result<Self, KafkaError> {
        engine
            .claim_assigned_consumer()
            .map(|handle| Self { handle })
            .map_err(translate_assigned_consumer_claim)
    }

    /// Attempts bounded close without consuming this capability on rejection.
    pub(crate) fn try_close(&mut self) -> Result<AssignedConsumerClose, KafkaError> {
        AssignedConsumerClose::from_admission(self.handle.try_close())
    }
}

impl std::fmt::Debug for AssignedConsumerEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerEngine")
            .finish_non_exhaustive()
    }
}
