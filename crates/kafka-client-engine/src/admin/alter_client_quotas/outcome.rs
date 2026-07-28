//! Stable engine terminal values for Admin `AlterClientQuotas`.

use core::fmt;

use super::AlterClientQuotaEntity;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka per-entity rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl AlterClientQuotaBrokerError {
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

/// One caller-ordered entity result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaOutcome {
    pub(super) entity: AlterClientQuotaEntity,
    pub(super) result: Result<(), AlterClientQuotaBrokerError>,
}

impl AlterClientQuotaOutcome {
    /// Returns the canonically identified quota entity.
    pub const fn entity(&self) -> &AlterClientQuotaEntity {
        &self.entity
    }

    /// Returns success or Kafka's exact per-entity rejection.
    pub const fn result(&self) -> &Result<(), AlterClientQuotaBrokerError> {
        &self.result
    }

    /// Consumes this row into its entity and result.
    pub fn into_parts(
        self,
    ) -> (
        AlterClientQuotaEntity,
        Result<(), AlterClientQuotaBrokerError>,
    ) {
        (self.entity, self.result)
    }
}

/// Caller-ordered entity outcomes plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<AlterClientQuotaOutcome>,
}

impl AlterClientQuotasBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-entity results in original request order.
    pub fn outcomes(&self) -> &[AlterClientQuotaOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and caller-ordered entity results.
    pub fn into_parts(self) -> (u32, Vec<AlterClientQuotaOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotasFailure {
    pub(super) kind: AlterClientQuotasFailureKind,
    pub(super) delivery: AlterClientQuotasDeliveryStatus,
}

impl AlterClientQuotasFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> AlterClientQuotasFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> AlterClientQuotasDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasOutcome {
    /// Kafka returned one caller-ordered result per requested entity.
    Altered(AlterClientQuotasBatch),
    /// The operation failed outside a valid entity-result batch.
    Failed(AlterClientQuotasFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AlterClientQuotasObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin AlterClientQuotas result was already observed",
            Self::Stale => "Admin AlterClientQuotas observer is stale",
        })
    }
}

impl std::error::Error for AlterClientQuotasObserverError {}
