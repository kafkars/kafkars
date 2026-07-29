//! Version-independent per-feature response facts for `UpdateFeatures`.

use super::UpdateFeaturesBrokerError;

/// Exact result for one requested finalized feature.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeatureResult {
    /// Kafka accepted or atomically validated this feature update.
    Updated,
    /// An older response rejected this feature with an exact signed code.
    Failed(UpdateFeaturesBrokerError),
}

/// One finalized-feature result retained in caller order after correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeatureOutcome {
    feature: String,
    result: UpdateFeatureResult,
}

impl UpdateFeatureOutcome {
    /// Creates one protocol-normalized successful feature result.
    pub const fn updated(feature: String) -> Self {
        Self {
            feature,
            result: UpdateFeatureResult::Updated,
        }
    }

    /// Creates one protocol-normalized per-feature broker failure.
    pub const fn failed(feature: String, error: UpdateFeaturesBrokerError) -> Self {
        Self {
            feature,
            result: UpdateFeatureResult::Failed(error),
        }
    }

    /// Returns the correlated finalized-feature name.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the exact normalized per-feature result.
    pub const fn result(&self) -> &UpdateFeatureResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, UpdateFeatureResult) {
        (self.feature, self.result)
    }
}

/// Caller-ordered finalized-feature results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesBatch {
    throttle_time_ms: u32,
    outcomes: Vec<UpdateFeatureOutcome>,
}

impl UpdateFeaturesBatch {
    /// Creates one protocol-normalized batch for deterministic correlation.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<UpdateFeatureOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one finalized-feature outcome per requested update.
    pub fn outcomes(&self) -> &[UpdateFeatureOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<UpdateFeatureOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Successful response semantics normalized across API versions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesBrokerResponse {
    /// Versions 0 and 1 returned independently settled per-feature results.
    FeatureResults(UpdateFeaturesBatch),
    /// Version 2 succeeded atomically and omitted per-feature result entries.
    AtomicSuccess {
        /// Kafka's nonnegative throttle observation.
        throttle_time_ms: u32,
    },
}
