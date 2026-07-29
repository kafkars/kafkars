//! Exact broker rejection and whole-operation terminal values for `DescribeFeatures`.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeFeaturesDescription;

/// Exact top-level API-18 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
}

impl DescribeFeaturesBrokerError {
    /// Creates one exact broker rejection from a normalized response.
    pub const fn new(throttle_time_ms: u32, code: NonZeroI16) -> Self {
        Self {
            throttle_time_ms,
            code,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }

    /// Consumes this rejection into exact scalar parts.
    pub const fn into_parts(self) -> (u32, i16) {
        (self.throttle_time_ms, self.code.get())
    }
}

/// Whole-operation failure outside an exact API-18 broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent complete feature metadata.
    Compatibility,
    /// A response was malformed or contradictory.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesFailure {
    kind: DescribeFeaturesFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeFeaturesFailure {
    pub(crate) const fn new(kind: DescribeFeaturesFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> DescribeFeaturesFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeFeatures`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesTerminal {
    /// Kafka returned complete bounded feature metadata.
    Described(DescribeFeaturesDescription),
    /// Kafka rejected the fixed request with an exact top-level code.
    BrokerRejected(DescribeFeaturesBrokerError),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeFeaturesFailure),
}
