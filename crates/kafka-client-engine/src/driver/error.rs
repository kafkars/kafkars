//! Construction failures at the client-to-driver ownership boundary.

use std::{error::Error, fmt};

use kafka_driver::{BootstrapError, CompletionError, DriverBuildError, ReactorError, SubmitError};

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
    /// A bounded embedded-reactor turn failed.
    Reactor(ReactorError),
    /// The driver's shared shutdown barrier rejected its first subscription.
    ShutdownSubmit(SubmitError),
    /// The retained shared shutdown barrier lost its terminal result.
    ShutdownCompletion(CompletionError),
    /// Bounded terminal turns could not reach driver shutdown.
    ShutdownTurnExhausted,
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
            Self::Reactor(source) => write!(formatter, "embedded driver turn failed: {source}"),
            Self::ShutdownSubmit(source) => {
                write!(formatter, "driver shutdown request was rejected: {source}")
            }
            Self::ShutdownCompletion(source) => {
                write!(formatter, "driver shutdown barrier failed: {source}")
            }
            Self::ShutdownTurnExhausted => {
                formatter.write_str("driver shutdown exceeded its bounded terminal turns")
            }
        }
    }
}

impl Error for DriverOwnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Endpoint { source, .. } => Some(source),
            Self::Bootstrap(source) => Some(source),
            Self::Build(source) => Some(source),
            Self::Reactor(source) => Some(source),
            Self::ShutdownSubmit(source) => Some(source),
            Self::ShutdownCompletion(source) => Some(source),
            Self::ShutdownTurnExhausted => None,
        }
    }
}
