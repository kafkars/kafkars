//! Cloneable public producer handle over the private engine bridge.

use std::time::Duration;

use crate::{ErrorKind, KafkaError, Record, bridge::producer::ProducerEngine};

use super::{CloseProducer, Delivery, Flush, Send, SendBatch, TrySendError};

/// Builder for a bounded, batch-native producer.
#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    engine: ProducerEngine,
}

impl ProducerBuilder {
    pub(crate) const fn new(engine: ProducerEngine) -> Self {
        Self { engine }
    }

    /// Sets the duration used to create each record's absolute end-to-end deadline.
    ///
    /// Each record operation converts this duration into an absolute deadline
    /// at its own public call boundary. It spans waiting, local batching,
    /// transport admission, and broker delivery.
    #[must_use]
    pub fn delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.engine = self.engine.with_delivery_timeout(delivery_timeout);
        self
    }

    /// Returns the selected default duration for each record operation.
    pub fn selected_delivery_timeout(&self) -> Duration {
        self.engine.delivery_timeout()
    }

    /// Builds the producer after local validation.
    pub fn build(self) -> Result<Producer, KafkaError> {
        let delivery_timeout = self.engine.delivery_timeout();
        if delivery_timeout.is_zero() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "producer delivery timeout must be nonzero",
            ));
        }
        if u64::try_from(delivery_timeout.as_nanos()).is_err() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "producer delivery timeout exceeds the supported range",
            ));
        }
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
    /// Sends one record through bounded FIFO waiting admission.
    ///
    /// The public deadline starts before record conversion. Local waiting has
    /// independent configured count and byte bounds; the client never retries
    /// `try_send` or polls the shard lock in a loop.
    pub fn send(&self, record: Record) -> Send {
        Send::from_bridge(self.engine.send(record))
    }

    /// Atomically closes admission and drains work accepted before this call.
    ///
    /// The first accepted close fences all clone-shared producer handles.
    /// Admission failures are immediately ready on the returned named
    /// operation; an accepted terminal observer remains the result authority.
    pub fn close(&self) -> CloseProducer {
        CloseProducer::from_bridge(self.engine.close())
    }

    /// Creates a barrier over records accepted before this call.
    ///
    /// Admission failures are returned by the named operation as an
    /// immediately-ready result. Dropping the operation abandons observation
    /// without cancelling accepted producer work.
    pub fn flush(&self) -> Flush {
        Flush::from_bridge(self.engine.flush())
    }

    /// Attempts immediate admission without waiting for local capacity.
    ///
    /// Explicit partitions admit directly; records without one enter bounded
    /// automatic partition resolution. Success transfers ownership to the
    /// engine and returns the sole terminal observer. Rejection returns the
    /// exact caller-owned record immediately.
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

    /// Admits an ordered record prefix through one batch-native boundary.
    ///
    /// All records are validated before admission. Thereafter, the first
    /// bounded rejection stops the call. The returned named operation observes
    /// every accepted-prefix delivery and retains the exact first rejected
    /// record plus untouched suffix.
    pub fn send_batch(&self, records: Vec<Record>) -> SendBatch {
        SendBatch::from_bridge(self.engine.send_batch(records))
    }
}
