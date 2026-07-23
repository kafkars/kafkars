//! Cloneable public producer handle over the private engine bridge.

use crate::{KafkaError, Record, bridge::producer::ProducerEngine};

use super::{Delivery, TrySendError};

/// Builder for a bounded, batch-native producer.
#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    engine: ProducerEngine,
}

impl ProducerBuilder {
    pub(crate) const fn new(engine: ProducerEngine) -> Self {
        Self { engine }
    }

    /// Builds the producer after local validation.
    pub fn build(self) -> Result<Producer, KafkaError> {
        Ok(Producer {
            engine: self.engine,
        })
    }
}

/// Cheaply cloneable, thread-safe producer handle.
#[derive(Debug, Clone)]
pub struct Producer {
    engine: ProducerEngine,
}

impl Producer {
    /// Attempts immediate admission without waiting for local capacity.
    ///
    /// The first vertical slice requires an explicit partition. Success
    /// transfers ownership to the engine and returns the sole terminal
    /// observer. Rejection returns the exact caller-owned record immediately.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failure returns the exact bytes-native record"
    )]
    pub fn try_send(&self, record: Record) -> Result<Delivery, TrySendError<Record>> {
        self.engine
            .try_send(record)
            .map(Delivery::from_bridge)
            .map_err(|rejection| {
                let (record, error) = rejection.into_parts();
                TrySendError::new(record, error)
            })
    }
}
