//! Stable engine terminal values for Admin `CreateAcls`.

use core::fmt;
use std::collections::TryReserveError;

mod translate;

pub(crate) use translate::{CreateAclsTranslationError, translate_terminal_into};

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka rejection for one requested ACL binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl CreateAclBrokerError {
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

/// Exact Kafka result for one requested binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclResult {
    /// Kafka accepted this binding.
    Created,
    /// Kafka rejected this binding with an exact error and diagnostic.
    BrokerFailed(CreateAclBrokerError),
}

/// One caller-ordered binding and its exact corresponding result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclOutcome {
    binding: super::CreateAclBinding,
    result: CreateAclResult,
}

impl CreateAclOutcome {
    /// Returns the requested binding.
    pub const fn binding(&self) -> &super::CreateAclBinding {
        &self.binding
    }

    /// Returns Kafka's exact corresponding result.
    pub const fn result(&self) -> &CreateAclResult {
        &self.result
    }

    /// Consumes this outcome into its stable binding and result.
    pub fn into_parts(self) -> (super::CreateAclBinding, CreateAclResult) {
        (self.binding, self.result)
    }
}

/// Caller-ordered settled binding results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<CreateAclOutcome>,
}

impl CreateAclsBatch {
    /// Fallibly reserves exact caller-order slots before operation admission.
    pub(crate) fn try_prepare_outcomes(
        required: usize,
    ) -> Result<Vec<CreateAclOutcome>, TryReserveError> {
        let mut outcomes = Vec::new();
        outcomes.try_reserve_exact(required)?;
        Ok(outcomes)
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns outcomes in exact request order.
    pub fn outcomes(&self) -> &[CreateAclOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and caller-ordered outcomes.
    pub fn into_parts(self) -> (u32, Vec<CreateAclOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsFailureKind {
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
    /// A response was malformed or could not be normalized.
    InvalidResponse,
}

/// Whole-operation mechanism failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateAclsFailure {
    kind: CreateAclsFailureKind,
    delivery: CreateAclsDeliveryStatus,
}

impl CreateAclsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> CreateAclsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> CreateAclsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclsOutcome {
    /// Every requested binding has a caller-ordered exact result.
    Created(CreateAclsBatch),
    /// The operation failed outside a complete per-binding result set.
    Failed(CreateAclsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for CreateAclsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin CreateAcls result was already observed",
            Self::Stale => "Admin CreateAcls observer is stale",
        })
    }
}

impl std::error::Error for CreateAclsObserverError {}
