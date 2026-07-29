//! Stable generated-free terminals for Admin `UpdateFeatures`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl UpdateFeaturesBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this rejection into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Stable result for one requested finalized feature.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeatureResult {
    /// Kafka accepted or atomically validated this feature update.
    Updated,
    /// An older broker rejected this feature independently.
    Failed(UpdateFeaturesBrokerError),
}

/// One finalized-feature result in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeatureOutcome {
    pub(super) feature: String,
    pub(super) result: UpdateFeatureResult,
}

impl UpdateFeatureOutcome {
    /// Returns the correlated finalized-feature name.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the exact per-feature result.
    pub const fn result(&self) -> &UpdateFeatureResult {
        &self.result
    }

    /// Consumes this row into its feature and result.
    pub fn into_parts(self) -> (String, UpdateFeatureResult) {
        (self.feature, self.result)
    }
}

/// Caller-ordered finalized-feature results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<UpdateFeatureOutcome>,
}

impl UpdateFeaturesBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one result per requested finalized feature.
    pub fn outcomes(&self) -> &[UpdateFeatureOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and caller-ordered feature results.
    pub fn into_parts(self) -> (u32, Vec<UpdateFeatureOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the sole prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the complete atomic operation.
    Broker(UpdateFeaturesBrokerError),
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesFailure {
    pub(super) kind: UpdateFeaturesFailureKind,
    pub(super) delivery: UpdateFeaturesDeliveryStatus,
}

impl UpdateFeaturesFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &UpdateFeaturesFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> UpdateFeaturesDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesOutcome {
    /// Kafka returned one result per requested finalized feature.
    Updated(UpdateFeaturesBatch),
    /// The operation failed outside a valid feature-result batch.
    Failed(UpdateFeaturesFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for UpdateFeaturesObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin UpdateFeatures result was already observed",
            Self::Stale => "Admin UpdateFeatures observer is stale",
        })
    }
}

impl std::error::Error for UpdateFeaturesObserverError {}
