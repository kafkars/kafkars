//! SASL mechanism and secret-retention policy.

use std::{fmt, sync::Arc};

use zeroize::Zeroize;

/// Stable SASL mechanism selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaslMechanism {
    /// SASL/PLAIN.
    Plain,
    /// SCRAM-SHA-256.
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

/// Broker credentials retained without exposing secret text in diagnostics.
#[must_use]
#[derive(Clone, Eq, PartialEq)]
pub struct Sasl {
    mechanism: SaslMechanism,
    username: Arc<str>,
    password: Arc<SecretText>,
}

impl Sasl {
    /// Creates SASL/PLAIN credentials.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanism::Plain, username, password)
    }

    /// Creates SCRAM-SHA-256 credentials.
    pub fn scram_sha_256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanism::ScramSha256, username, password)
    }

    /// Creates SCRAM-SHA-512 credentials.
    pub fn scram_sha_512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(SaslMechanism::ScramSha512, username, password)
    }

    /// Returns the configured authentication mechanism without exposing credentials.
    pub const fn mechanism(&self) -> SaslMechanism {
        self.mechanism
    }

    fn new(
        mechanism: SaslMechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            mechanism,
            username: Arc::from(username.into()),
            password: Arc::new(SecretText(password.into())),
        }
    }

    pub(crate) fn credentials(&self) -> (&str, &str) {
        (&self.username, &self.password.0)
    }
}

impl fmt::Debug for Sasl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sasl")
            .field("mechanism", &self.mechanism)
            .finish_non_exhaustive()
    }
}

#[derive(Eq, PartialEq)]
struct SecretText(String);

impl Drop for SecretText {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}
