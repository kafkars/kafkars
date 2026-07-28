//! TLS trust selection for logical broker identities.

use std::{fmt, sync::Arc};

/// TLS trust policy for Kafka broker connections.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct Tls {
    trust: TlsTrust,
}

#[derive(Clone, Default, Eq, PartialEq)]
enum TlsTrust {
    #[default]
    SystemRoots,
    CustomRootsPem(Arc<[u8]>),
}

impl Tls {
    /// Uses a snapshot of the platform-native certificate roots at client startup.
    pub const fn system_roots() -> Self {
        Self {
            trust: TlsTrust::SystemRoots,
        }
    }

    /// Uses only the certificates in one PEM bundle as trusted roots.
    ///
    /// Parsing and bounded validation happen when the client starts. The
    /// bundle may contain more than one `CERTIFICATE` section.
    pub fn custom_roots_pem(pem: impl Into<Vec<u8>>) -> Self {
        Self {
            trust: TlsTrust::CustomRootsPem(Arc::from(pem.into())),
        }
    }

    pub(crate) fn custom_roots_pem_bytes(&self) -> Option<&[u8]> {
        match &self.trust {
            TlsTrust::SystemRoots => None,
            TlsTrust::CustomRootsPem(pem) => Some(pem),
        }
    }
}

impl fmt::Debug for Tls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut diagnostic = formatter.debug_struct("Tls");
        match &self.trust {
            TlsTrust::SystemRoots => {
                diagnostic.field("trust", &"SystemRoots");
            }
            TlsTrust::CustomRootsPem(pem) => {
                diagnostic
                    .field("trust", &"CustomRootsPem")
                    .field("pem_bytes", &pem.len());
            }
        }
        diagnostic.finish()
    }
}
