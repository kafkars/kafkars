//! Caller-ordered client-quota alteration outcomes with throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

use super::ClientQuotaEntity;

/// Fully settled client-quota entity alterations in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasResult {
    throttle_time: Duration,
    entities: BatchResult<ClientQuotaEntity, ()>,
}

impl AlterClientQuotasResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        entities: BatchResult<ClientQuotaEntity, ()>,
    ) -> Self {
        Self {
            throttle_time,
            entities,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns entity outcomes in original request order.
    pub const fn entities(&self) -> &BatchResult<ClientQuotaEntity, ()> {
        &self.entities
    }

    /// Consumes the result into caller-ordered entity outcomes.
    pub fn into_entities(self) -> BatchResult<ClientQuotaEntity, ()> {
        self.entities
    }
}
