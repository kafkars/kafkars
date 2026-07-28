//! Stable non-secret engine terminals for Admin `DescribeUserScramCredentials`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsDeliveryStatus {
    /// The failed call did not reach Kafka.
    NotSent,
    /// The failed call may have reached Kafka.
    PossiblySent,
}

/// One exact SCRAM mechanism code and positive iteration count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialInfo {
    pub(super) mechanism: i8,
    pub(super) iterations: u32,
}

impl DescribeUserScramCredentialInfo {
    /// Returns Kafka's exact signed SCRAM mechanism code.
    pub const fn mechanism(self) -> i8 {
        self.mechanism
    }

    /// Returns Kafka's positive iteration count.
    pub const fn iterations(self) -> u32 {
        self.iterations
    }

    /// Consumes this metadata into exact scalar parts.
    pub const fn into_parts(self) -> (i8, u32) {
        (self.mechanism, self.iterations)
    }
}

/// Exact Kafka rejection and bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl DescribeUserScramCredentialsBrokerError {
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

    /// Consumes the rejection into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact result Kafka returned for one correlated user.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsUserResult {
    /// Kafka returned non-secret mechanism and iteration metadata.
    Described(Vec<DescribeUserScramCredentialInfo>),
    /// Kafka rejected this user with an exact signed code.
    BrokerFailed(DescribeUserScramCredentialsBrokerError),
}

/// One deterministic user result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialOutcome {
    pub(super) user: String,
    pub(super) result: DescribeUserScramCredentialsUserResult,
}

impl DescribeUserScramCredentialOutcome {
    /// Returns the correlated user identity.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns this user's exact result.
    pub const fn result(&self) -> &DescribeUserScramCredentialsUserResult {
        &self.result
    }

    /// Consumes this outcome into stable user and result parts.
    pub fn into_parts(self) -> (String, DescribeUserScramCredentialsUserResult) {
        (self.user, self.result)
    }
}

/// Deterministically ordered per-user results plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<DescribeUserScramCredentialOutcome>,
}

impl DescribeUserScramCredentialsBatch {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns explicit-filter caller order or all-user byte order.
    pub fn outcomes(&self) -> &[DescribeUserScramCredentialOutcome] {
        &self.outcomes
    }

    /// Consumes throttle and deterministically ordered per-user results.
    pub fn into_parts(self) -> (u32, Vec<DescribeUserScramCredentialOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Stable whole-operation failure category.
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
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected API version cannot represent required semantics.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsFailure {
    pub(super) kind: DescribeUserScramCredentialsFailureKind,
    pub(super) delivery: DescribeUserScramCredentialsDeliveryStatus,
}

impl DescribeUserScramCredentialsFailure {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> &DescribeUserScramCredentialsFailureKind {
        &self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(&self) -> DescribeUserScramCredentialsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsOutcome {
    /// Kafka returned zero or more deterministic non-secret user results.
    Described(DescribeUserScramCredentialsBatch),
    /// The operation failed outside a valid user-result set.
    Failed(DescribeUserScramCredentialsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeUserScramCredentialsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => {
                "Admin DescribeUserScramCredentials result was already observed"
            }
            Self::Stale => "Admin DescribeUserScramCredentials observer is stale",
        })
    }
}

impl std::error::Error for DescribeUserScramCredentialsObserverError {}
