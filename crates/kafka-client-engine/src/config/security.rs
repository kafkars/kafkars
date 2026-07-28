//! Client-owned security policy validated into private driver configuration.

use std::{fmt, sync::Arc};

use zeroize::Zeroize;

/// Engine-owned TLS trust selection.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct EngineTls {
    trust: EngineTlsTrust,
}

#[derive(Clone, Default, Eq, PartialEq)]
enum EngineTlsTrust {
    #[default]
    SystemRoots,
    CustomRootsPem(Arc<[u8]>),
}

impl EngineTls {
    /// Uses a startup snapshot of the platform-native certificate roots.
    pub const fn system_roots() -> Self {
        Self {
            trust: EngineTlsTrust::SystemRoots,
        }
    }

    /// Retains one caller-owned PEM certificate bundle for startup validation.
    pub fn custom_roots_pem(pem: impl Into<Vec<u8>>) -> Self {
        Self {
            trust: EngineTlsTrust::CustomRootsPem(Arc::from(pem.into())),
        }
    }

    pub(crate) fn custom_roots_pem_bytes(&self) -> Option<&[u8]> {
        match &self.trust {
            EngineTlsTrust::SystemRoots => None,
            EngineTlsTrust::CustomRootsPem(pem) => Some(pem),
        }
    }
}

impl fmt::Debug for EngineTls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut diagnostic = formatter.debug_struct("EngineTls");
        match &self.trust {
            EngineTlsTrust::SystemRoots => {
                diagnostic.field("trust", &"SystemRoots");
            }
            EngineTlsTrust::CustomRootsPem(pem) => {
                diagnostic
                    .field("trust", &"CustomRootsPem")
                    .field("pem_bytes", &pem.len());
            }
        }
        diagnostic.finish()
    }
}

/// Engine-owned SASL mechanism vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineSaslMechanism {
    /// SASL/PLAIN.
    Plain,
    /// SCRAM-SHA-256.
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

/// Engine-owned credentials with redacted diagnostics and zeroized final release.
#[derive(Clone, Eq, PartialEq)]
pub struct EngineSasl {
    mechanism: EngineSaslMechanism,
    username: Arc<str>,
    password: Arc<SecretText>,
}

impl EngineSasl {
    /// Creates unvalidated SASL/PLAIN credentials for engine startup validation.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(EngineSaslMechanism::Plain, username, password)
    }

    /// Creates unvalidated SCRAM-SHA-256 credentials for engine startup validation.
    pub fn scram_sha_256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(EngineSaslMechanism::ScramSha256, username, password)
    }

    /// Creates unvalidated SCRAM-SHA-512 credentials for engine startup validation.
    pub fn scram_sha_512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(EngineSaslMechanism::ScramSha512, username, password)
    }

    /// Returns the configured mechanism without exposing credentials.
    pub const fn mechanism(&self) -> EngineSaslMechanism {
        self.mechanism
    }

    fn new(
        mechanism: EngineSaslMechanism,
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

impl fmt::Debug for EngineSasl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineSasl")
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

/// Complete engine-owned transport and authentication policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EngineSecurity {
    /// Plain TCP without SASL.
    Plaintext,
    /// TLS without SASL.
    Tls(EngineTls),
    /// SASL over plain TCP.
    SaslPlaintext(EngineSasl),
    /// SASL over TLS.
    SaslTls {
        /// TLS trust used by each logical broker identity.
        tls: EngineTls,
        /// Broker authentication mechanism and credentials.
        sasl: EngineSasl,
    },
}

impl EngineSecurity {
    /// Selects plain TCP without broker authentication.
    pub const fn plaintext() -> Self {
        Self::Plaintext
    }

    /// Selects TLS transport without SASL.
    pub const fn tls(tls: EngineTls) -> Self {
        Self::Tls(tls)
    }

    /// Selects SASL over plain TCP.
    pub const fn sasl_plaintext(sasl: EngineSasl) -> Self {
        Self::SaslPlaintext(sasl)
    }

    /// Selects SASL over TLS.
    pub const fn sasl_tls(tls: EngineTls, sasl: EngineSasl) -> Self {
        Self::SaslTls { tls, sasl }
    }
}

impl Default for EngineSecurity {
    fn default() -> Self {
        Self::Plaintext
    }
}
