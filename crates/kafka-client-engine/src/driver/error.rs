//! Construction failures at the client-to-driver ownership boundary.

use std::{error::Error, fmt};

use kafka_driver::{BootstrapError, DriverBuildError};

use super::EndpointError;

/// Why the engine could not acquire its unique embedded driver reactor.
#[derive(Debug)]
pub(crate) enum DriverOwnerError {
    /// One configured bootstrap entry was not a driver endpoint.
    Endpoint {
        /// Zero-based position in the configured bootstrap sequence.
        index: usize,
        /// Endpoint translation failure.
        source: EndpointError,
    },
    /// The driver's bounded bootstrap set rejected the complete sequence.
    Bootstrap(BootstrapError),
    /// The driver could not acquire its embedded reactor resources.
    Build(DriverBuildError),
}

impl fmt::Display for DriverOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint { index, source } => {
                write!(
                    formatter,
                    "invalid bootstrap endpoint at index {index}: {source}"
                )
            }
            Self::Bootstrap(source) => write!(formatter, "invalid bootstrap set: {source}"),
            Self::Build(source) => write!(formatter, "failed to build embedded driver: {source}"),
        }
    }
}

impl Error for DriverOwnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Endpoint { source, .. } => Some(source),
            Self::Bootstrap(source) => Some(source),
            Self::Build(source) => Some(source),
        }
    }
}
