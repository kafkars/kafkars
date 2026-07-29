//! Borrowed secret input and generated-free normalized result facts.

use core::fmt;

/// One deletion or password-based SCRAM credential upsertion.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialAlterationRef<'a> {
    Delete {
        user: &'a str,
        mechanism: i8,
    },
    Upsert {
        user: &'a str,
        mechanism: i8,
        iterations: u32,
        password: &'a [u8],
        salt: Option<&'a [u8]>,
    },
}

impl<'a> AlterUserScramCredentialAlterationRef<'a> {
    pub(crate) const fn delete(user: &'a str, mechanism: i8) -> Self {
        Self::Delete { user, mechanism }
    }

    pub(crate) const fn upsert(
        user: &'a str,
        mechanism: i8,
        iterations: u32,
        password: &'a [u8],
        salt: Option<&'a [u8]>,
    ) -> Self {
        Self::Upsert {
            user,
            mechanism,
            iterations,
            password,
            salt,
        }
    }

    pub(crate) const fn user(self) -> &'a str {
        match self {
            Self::Delete { user, .. } | Self::Upsert { user, .. } => user,
        }
    }

    pub(crate) const fn mechanism(self) -> i8 {
        match self {
            Self::Delete { mechanism, .. } | Self::Upsert { mechanism, .. } => mechanism,
        }
    }
}

impl fmt::Debug for AlterUserScramCredentialAlterationRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delete { user, mechanism } => formatter
                .debug_struct("Delete")
                .field("user", user)
                .field("mechanism", mechanism)
                .finish(),
            Self::Upsert {
                user,
                mechanism,
                iterations,
                salt,
                ..
            } => formatter
                .debug_struct("Upsert")
                .field("user", user)
                .field("mechanism", mechanism)
                .field("iterations", iterations)
                .field("password", &"[REDACTED]")
                .field("salt", &salt.map(<[u8]>::len))
                .finish(),
        }
    }
}

/// One complete borrowed, caller-ordered API-key 51 input.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct AlterUserScramCredentialsRequestRef<'a> {
    alterations: &'a [AlterUserScramCredentialAlterationRef<'a>],
}

impl<'a> AlterUserScramCredentialsRequestRef<'a> {
    pub(crate) const fn new(alterations: &'a [AlterUserScramCredentialAlterationRef<'a>]) -> Self {
        Self { alterations }
    }

    pub(crate) const fn alterations(self) -> &'a [AlterUserScramCredentialAlterationRef<'a>] {
        self.alterations
    }
}

impl fmt::Debug for AlterUserScramCredentialsRequestRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsRequestRef")
            .field("alterations", &self.alterations)
            .finish()
    }
}

/// Non-secret response-correlation facts retained by the core plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlterUserScramCredentialsCorrelationRef<'a> {
    affected_users: &'a [String],
}

impl<'a> AlterUserScramCredentialsCorrelationRef<'a> {
    pub(crate) const fn new(affected_users: &'a [String]) -> Self {
        Self { affected_users }
    }

    pub(crate) const fn affected_users(self) -> &'a [String] {
        self.affected_users
    }
}

/// One caller-correlated user outcome with an exact signed broker code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterUserScramCredentialOutcome {
    pub(super) user: String,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
}

impl NormalizedAlterUserScramCredentialOutcome {
    pub(crate) fn into_parts(self) -> (String, i16, Option<String>, bool) {
        (
            self.user,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
        )
    }
}

/// One exact-v0 response restored to first-occurrence user order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterUserScramCredentialsResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<NormalizedAlterUserScramCredentialOutcome>,
    pub(super) retained_bytes: usize,
}

impl NormalizedAlterUserScramCredentialsResponse {
    pub(crate) fn into_parts(self) -> (u32, Vec<NormalizedAlterUserScramCredentialOutcome>, usize) {
        (self.throttle_time_ms, self.outcomes, self.retained_bytes)
    }
}
