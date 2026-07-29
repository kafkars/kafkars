//! Caller-ordered finalized-feature outcomes with throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

/// Fully settled finalized-feature updates in original request order.
///
/// Older brokers may return a mixture of successful and failed feature
/// entries. Failures that prevent a valid per-feature result set are returned
/// by [`super::UpdateFeatures`] as one `KafkaError` retaining delivery
/// certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesResult {
    throttle_time: Duration,
    features: BatchResult<String, ()>,
}

impl UpdateFeaturesResult {
    pub(crate) const fn new(throttle_time: Duration, features: BatchResult<String, ()>) -> Self {
        Self {
            throttle_time,
            features,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-feature outcomes in original request order.
    pub const fn features(&self) -> &BatchResult<String, ()> {
        &self.features
    }

    /// Consumes the result into caller-ordered per-feature outcomes.
    pub fn into_features(self) -> BatchResult<String, ()> {
        self.features
    }
}
