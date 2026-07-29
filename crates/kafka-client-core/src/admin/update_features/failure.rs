//! Whole-operation failures and the sole terminal value for `UpdateFeatures`.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::UpdateFeaturesBatch;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const UPDATE_FEATURES_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker rejection for one feature or the complete operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl UpdateFeaturesBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into exact adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Whole-operation failure outside a valid correlated feature result set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the complete atomic operation.
    Broker(UpdateFeaturesBrokerError),
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
    /// The negotiated API cannot represent the requested intent.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateFeaturesFailure {
    kind: UpdateFeaturesFailureKind,
    delivery: DeliveryStatus,
}

impl UpdateFeaturesFailure {
    pub(crate) const fn new(kind: UpdateFeaturesFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(&self) -> &UpdateFeaturesFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty without inventing retry policy.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `UpdateFeatures`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesTerminal {
    /// Every requested feature owns a caller-ordered result.
    Updated(UpdateFeaturesBatch),
    /// The whole operation failed outside a valid per-feature result set.
    Failed(UpdateFeaturesFailure),
}
