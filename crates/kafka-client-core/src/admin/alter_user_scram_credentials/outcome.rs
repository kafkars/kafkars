//! Caller-ordered per-user outcomes and terminal facts for SCRAM alteration.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker-declared failure for one affected user.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AlterUserScramCredentialBrokerError {
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

/// Per-user result of one SCRAM credential alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialResult {
    /// Kafka accepted every requested mechanism change for this user.
    Altered,
    /// Kafka rejected this user with an exact signed code.
    Failed(AlterUserScramCredentialBrokerError),
}

/// One affected-user result retained in first-occurrence request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialOutcome {
    user: String,
    result: AlterUserScramCredentialResult,
}

impl AlterUserScramCredentialOutcome {
    /// Creates one protocol-normalized successful user result.
    pub const fn altered(user: String) -> Self {
        Self {
            user,
            result: AlterUserScramCredentialResult::Altered,
        }
    }

    /// Creates one protocol-normalized broker-rejected user result.
    pub const fn failed(user: String, error: AlterUserScramCredentialBrokerError) -> Self {
        Self {
            user,
            result: AlterUserScramCredentialResult::Failed(error),
        }
    }

    /// Returns the correlated affected user.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns the exact per-user result.
    pub const fn result(&self) -> &AlterUserScramCredentialResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, AlterUserScramCredentialResult) {
        (self.user, self.result)
    }
}

/// One successful correlated batch plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AlterUserScramCredentialOutcome>,
}

impl AlterUserScramCredentialsBatch {
    /// Creates one protocol-normalized response batch for core correlation.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AlterUserScramCredentialOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per distinct affected user.
    pub fn outcomes(&self) -> &[AlterUserScramCredentialOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AlterUserScramCredentialOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure outside a valid correlated user result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsFailureKind {
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
pub struct AlterUserScramCredentialsFailure {
    kind: AlterUserScramCredentialsFailureKind,
    delivery: DeliveryStatus,
}

impl AlterUserScramCredentialsFailure {
    pub(crate) const fn new(
        kind: AlterUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the deterministic failure category.
    pub const fn kind(self) -> AlterUserScramCredentialsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `AlterUserScramCredentials`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsTerminal {
    /// Kafka returned exactly one result per affected user.
    Altered(AlterUserScramCredentialsBatch),
    /// The whole operation failed outside a valid user result set.
    Failed(AlterUserScramCredentialsFailure),
}
