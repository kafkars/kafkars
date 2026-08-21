//! Stable producer identity and ordered fencing batch result.

use std::time::Duration;

use crate::KafkaError;
use crate::admin::BatchResult;

/// Exact broker producer identity returned after fencing a transactional ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FencedProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl FencedProducerIdentity {
    pub(crate) const fn new(producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
        }
    }

    /// Returns Kafka's signed producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's signed producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}

/// Caller-ordered fencing outcomes with the maximum observed broker throttle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceProducersResult {
    throttle_time: Duration,
    producers: BatchResult<String, FencedProducerIdentity>,
}

impl FenceProducersResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        producers: BatchResult<String, FencedProducerIdentity>,
    ) -> Self {
        Self {
            throttle_time,
            producers,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-transactional-ID outcomes in original caller order.
    pub const fn producers(&self) -> &BatchResult<String, FencedProducerIdentity> {
        &self.producers
    }

    /// Consumes this result into caller-ordered per-transactional-ID outcomes.
    pub fn into_producers(self) -> BatchResult<String, FencedProducerIdentity> {
        self.producers
    }

    /// Returns outcomes in original caller order.
    pub fn entries(&self) -> &[(String, Result<FencedProducerIdentity, KafkaError>)] {
        self.producers.entries()
    }

    /// Consumes outcomes in original caller order.
    pub fn into_entries(self) -> Vec<(String, Result<FencedProducerIdentity, KafkaError>)> {
        self.producers.into_entries()
    }
}
