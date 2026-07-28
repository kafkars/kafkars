//! User-correlated SCRAM credential metadata and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::ScramCredentialInfo;

/// Maximum retained UTF-8 broker diagnostic prefix.
pub const DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker rejection for one user or the complete operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeUserScramCredentialsBrokerError {
    /// Creates one exact signed Kafka error with an already-bounded diagnostic.
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

    /// Consumes this error into exact adapter-owned parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result Kafka returned for one user.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsUserResult {
    /// Kafka described only mechanism and iteration metadata.
    Described(Vec<ScramCredentialInfo>),
    /// Kafka rejected this user with an exact signed code.
    BrokerFailed(DescribeUserScramCredentialsBrokerError),
}

/// One result retained with its correlated user identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsUserOutcome {
    user: String,
    result: DescribeUserScramCredentialsUserResult,
}

impl DescribeUserScramCredentialsUserOutcome {
    /// Creates one successful user result containing no credential secrets.
    pub const fn described(user: String, credentials: Vec<ScramCredentialInfo>) -> Self {
        Self {
            user,
            result: DescribeUserScramCredentialsUserResult::Described(credentials),
        }
    }

    /// Creates one exact per-user broker failure.
    pub const fn broker_failed(
        user: String,
        error: DescribeUserScramCredentialsBrokerError,
    ) -> Self {
        Self {
            user,
            result: DescribeUserScramCredentialsUserResult::BrokerFailed(error),
        }
    }

    /// Returns the correlated user identity.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns the exact per-user result.
    pub const fn result(&self) -> &DescribeUserScramCredentialsUserResult {
        &self.result
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, DescribeUserScramCredentialsUserResult) {
        (self.user, self.result)
    }

    pub(crate) fn credentials_mut(&mut self) -> Option<&mut Vec<ScramCredentialInfo>> {
        match &mut self.result {
            DescribeUserScramCredentialsUserResult::Described(credentials) => Some(credentials),
            DescribeUserScramCredentialsUserResult::BrokerFailed(_) => None,
        }
    }
}

/// Deterministically ordered user results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeUserScramCredentialsUserOutcome>,
}

impl DescribeUserScramCredentialsBatch {
    /// Creates one protocol-normalized batch for deterministic validation.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<DescribeUserScramCredentialsUserOutcome>,
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

    /// Returns user results in caller or canonical all-user order.
    pub fn outcomes(&self) -> &[DescribeUserScramCredentialsUserOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into throttle and ordered user results.
    pub fn into_parts(self) -> (u32, Vec<DescribeUserScramCredentialsUserOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }

    pub(crate) fn outcomes_mut(&mut self) -> &mut Vec<DescribeUserScramCredentialsUserOutcome> {
        &mut self.outcomes
    }
}

/// Whole-operation failure outside exact per-user results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// Kafka rejected the complete query with an exact top-level error.
    Broker(DescribeUserScramCredentialsBrokerError),
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsFailure {
    kind: DescribeUserScramCredentialsFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeUserScramCredentialsFailure {
    pub(crate) const fn new(
        kind: DescribeUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the core-owned failure category.
    pub const fn kind(&self) -> &DescribeUserScramCredentialsFailureKind {
        &self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(&self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DescribeUserScramCredentials`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsTerminal {
    /// Kafka returned zero or more deterministically ordered user results.
    Described(DescribeUserScramCredentialsBatch),
    /// The whole operation failed outside a valid user result set.
    Failed(DescribeUserScramCredentialsFailure),
}
