//! Caller-ordered per-entity outcomes and terminal facts for client-quota alteration.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::AlterClientQuotaEntity;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const ALTER_CLIENT_QUOTAS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker-declared failure for one requested entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AlterClientQuotaBrokerError {
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

    /// Returns the nullable UTF-8-safe diagnostic prefix.
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

/// Per-entity result of one client-quota alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterClientQuotaResult {
    /// Kafka accepted or validate-only checked every operation for this entity.
    Altered,
    /// Kafka rejected this entity with an exact signed code.
    Failed(AlterClientQuotaBrokerError),
}

/// One entity result retained in original request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaOutcome {
    entity: AlterClientQuotaEntity,
    result: AlterClientQuotaResult,
}

impl AlterClientQuotaOutcome {
    /// Creates one protocol-normalized successful entity result.
    pub const fn altered(entity: AlterClientQuotaEntity) -> Self {
        Self {
            entity,
            result: AlterClientQuotaResult::Altered,
        }
    }

    /// Creates one protocol-normalized broker-rejected entity result.
    pub const fn failed(
        entity: AlterClientQuotaEntity,
        error: AlterClientQuotaBrokerError,
    ) -> Self {
        Self {
            entity,
            result: AlterClientQuotaResult::Failed(error),
        }
    }

    /// Returns the canonical entity identity after core validation.
    pub const fn entity(&self) -> &AlterClientQuotaEntity {
        &self.entity
    }

    /// Returns the exact per-entity result.
    pub const fn result(&self) -> &AlterClientQuotaResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (AlterClientQuotaEntity, AlterClientQuotaResult) {
        (self.entity, self.result)
    }
}

/// One successful correlated batch plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterClientQuotaOutcome>,
}

impl AlterClientQuotasBatch {
    /// Creates one protocol-normalized response batch for core correlation.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<AlterClientQuotaOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per request entry in caller order.
    pub fn outcomes(&self) -> &[AlterClientQuotaOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AlterClientQuotaOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure outside a valid correlated entity result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A structurally valid response exceeded retained terminal capacity.
    ResponseTooLarge,
    /// The negotiated API cannot represent required alteration semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasFailure {
    kind: AlterClientQuotasFailureKind,
    delivery: DeliveryStatus,
}

impl AlterClientQuotasFailure {
    pub(crate) const fn new(kind: AlterClientQuotasFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> AlterClientQuotasFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `AlterClientQuotas`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasTerminal {
    /// Kafka returned exactly one result per request entity in caller order.
    Altered(AlterClientQuotasBatch),
    /// The whole operation failed outside a valid entity result set.
    Failed(AlterClientQuotasFailure),
}
