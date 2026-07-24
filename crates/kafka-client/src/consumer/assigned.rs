//! Unique public ownership of one directly assigned consumer.

use crate::bridge::consumer::AssignedConsumerEngine;

use super::CloseAssignedConsumer;

/// Consumer whose positions are controlled directly rather than by a group.
///
/// Assignment and record delivery remain absent until their engine seams are
/// complete; this handle currently exposes only its real close lifecycle.
#[derive(Debug)]
pub struct AssignedConsumer {
    engine: AssignedConsumerEngine,
}

impl AssignedConsumer {
    pub(crate) const fn new(engine: AssignedConsumerEngine) -> Self {
        Self { engine }
    }

    /// Attempts to close this consumer and returns the sole terminal observer.
    ///
    /// Close admission reserves its terminal capacity before deterministic core
    /// policy closes later work. Rejection leaves this unique consumer available
    /// for an explicit retry.
    pub fn try_close(&mut self) -> Result<CloseAssignedConsumer, crate::KafkaError> {
        self.engine
            .try_close()
            .map(CloseAssignedConsumer::from_bridge)
    }
}
