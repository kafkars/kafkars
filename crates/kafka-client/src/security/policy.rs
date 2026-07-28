//! Complete transport and authentication policy for one shared client.

use super::{Sasl, Tls};

/// One complete transport and authentication policy for the shared client.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Security {
    /// Plain TCP without broker authentication.
    Plaintext,
    /// TLS transport without SASL.
    Tls(Tls),
    /// Plain TCP with SASL authentication.
    SaslPlaintext(Sasl),
    /// TLS transport with SASL authentication.
    SaslTls {
        /// TLS trust used by each logical broker identity.
        tls: Tls,
        /// Broker authentication mechanism and credentials.
        sasl: Sasl,
    },
}

impl Security {
    /// Selects plain TCP without broker authentication.
    pub const fn plaintext() -> Self {
        Self::Plaintext
    }

    /// Selects TLS transport without SASL.
    pub const fn tls(tls: Tls) -> Self {
        Self::Tls(tls)
    }

    /// Selects SASL over plain TCP.
    pub const fn sasl_plaintext(sasl: Sasl) -> Self {
        Self::SaslPlaintext(sasl)
    }

    /// Selects SASL over TLS.
    pub const fn sasl_tls(tls: Tls, sasl: Sasl) -> Self {
        Self::SaslTls { tls, sasl }
    }
}

impl Default for Security {
    fn default() -> Self {
        Self::Plaintext
    }
}
