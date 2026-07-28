//! Deterministically ordered client-quota entities and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::DescribeClientQuotaEntity;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const DESCRIBE_CLIENT_QUOTAS_DIAGNOSTIC_BYTES: usize = 1024;

/// Successful deterministic entity set plus Kafka's throttle observation.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotasBatch {
    throttle_time_ms: u32,
    entities: Vec<DescribeClientQuotaEntity>,
}

impl DescribeClientQuotasBatch {
    /// Creates one protocol-normalized batch for deterministic core validation.
    pub const fn new(throttle_time_ms: u32, entities: Vec<DescribeClientQuotaEntity>) -> Self {
        Self {
            throttle_time_ms,
            entities,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns quota entities in deterministic canonical-identity order.
    pub fn entities(&self) -> &[DescribeClientQuotaEntity] {
        &self.entities
    }

    /// Consumes the batch into throttle and ordered entities.
    pub fn into_parts(self) -> (u32, Vec<DescribeClientQuotaEntity>) {
        (self.throttle_time_ms, self.entities)
    }

    pub(crate) fn canonicalize(&mut self) {
        for entity in &mut self.entities {
            entity.canonicalize();
        }
        self.entities
            .sort_unstable_by(DescribeClientQuotaEntity::deterministic_cmp);
    }
}

/// Exact broker-declared top-level error and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeClientQuotasBrokerError {
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

    /// Consumes the error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Whole-operation failure outside a valid client-quota entity set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the query with an exact top-level error.
    Broker(DescribeClientQuotasBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasFailure {
    kind: DescribeClientQuotasFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeClientQuotasFailure {
    pub(crate) const fn new(
        kind: DescribeClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> &DescribeClientQuotasFailureKind {
        &self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeClientQuotas`.
#[derive(Clone, Debug, PartialEq)]
pub enum DescribeClientQuotasTerminal {
    /// Kafka returned zero or more deterministically ordered quota entities.
    Described(DescribeClientQuotasBatch),
    /// The whole operation failed outside a valid entity set.
    Failed(DescribeClientQuotasFailure),
}
