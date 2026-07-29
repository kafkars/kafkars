//! Stable engine terminal values for Admin `AlterUserScramCredentials`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// Exact Kafka per-user rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl AlterUserScramCredentialBrokerError {
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

/// One affected-user result in first-occurrence request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialOutcome {
    pub(super) user: String,
    pub(super) result: Result<(), AlterUserScramCredentialBrokerError>,
}

impl AlterUserScramCredentialOutcome {
    /// Returns the correlated Kafka user.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns success or Kafka's exact per-user rejection.
    pub const fn result(&self) -> &Result<(), AlterUserScramCredentialBrokerError> {
        &self.result
    }

    /// Consumes this row into its user and result.
    pub fn into_parts(self) -> (String, Result<(), AlterUserScramCredentialBrokerError>) {
        (self.user, self.result)
    }
}

/// First-occurrence user outcomes plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterUserScramCredentialsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<AlterUserScramCredentialOutcome>,
}

impl AlterUserScramCredentialsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns one result per distinct affected user.
    pub fn outcomes(&self) -> &[AlterUserScramCredentialOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and first-occurrence user results.
    pub fn into_parts(self) -> (u32, Vec<AlterUserScramCredentialOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsFailureKind {
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
pub struct AlterUserScramCredentialsFailure {
    pub(super) kind: AlterUserScramCredentialsFailureKind,
    pub(super) delivery: AlterUserScramCredentialsDeliveryStatus,
}

impl AlterUserScramCredentialsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> AlterUserScramCredentialsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> AlterUserScramCredentialsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsOutcome {
    /// Kafka returned one result per distinct affected user.
    Altered(AlterUserScramCredentialsBatch),
    /// The operation failed outside a valid user-result batch.
    Failed(AlterUserScramCredentialsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AlterUserScramCredentialsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin AlterUserScramCredentials result was already observed",
            Self::Stale => "Admin AlterUserScramCredentials observer is stale",
        })
    }
}

impl std::error::Error for AlterUserScramCredentialsObserverError {}
