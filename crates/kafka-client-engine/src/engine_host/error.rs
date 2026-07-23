//! Stable startup failures and internal terminal host diagnostics.

use std::fmt;

use crate::{
    clock::ClockError,
    completion::{CompletionRegistryError, NotifierJoinError},
    config::EngineConfigError,
    driver::DriverOwnerError,
    producer::{ProducerExecutionStopError, ProducerHostInvariantError, ProducerHostStartError},
};

/// Stable category for engine startup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineStartErrorKind {
    /// Engine-owned configuration is invalid.
    Configuration,
    /// The embedded driver could not be acquired.
    Driver,
    /// Bounded producer resources could not be acquired.
    Producer,
    /// The native engine host thread could not start.
    HostThread,
    /// Startup ownership could not be handed to the host thread.
    HostHandoff,
}

/// Failure before an engine handle can become observable.
#[derive(Debug)]
pub struct EngineStartError {
    kind: EngineStartErrorKind,
    detail: String,
}

impl EngineStartError {
    /// Returns the stable startup-failure category.
    pub const fn kind(&self) -> EngineStartErrorKind {
        self.kind
    }

    pub(crate) fn configuration(error: EngineConfigError) -> Self {
        Self::new(
            EngineStartErrorKind::Configuration,
            format!("invalid engine configuration: {error:?}"),
        )
    }

    pub(super) fn driver(error: &DriverOwnerError) -> Self {
        Self::new(EngineStartErrorKind::Driver, error.to_string())
    }

    pub(super) fn producer(error: &ProducerHostStartError) -> Self {
        Self::new(EngineStartErrorKind::Producer, error.to_string())
    }

    pub(super) fn host_thread(error: &std::io::Error) -> Self {
        Self::new(
            EngineStartErrorKind::HostThread,
            format!("failed to start engine host thread: {error}"),
        )
    }

    pub(super) fn handoff() -> Self {
        Self::new(
            EngineStartErrorKind::HostHandoff,
            "engine host stopped before accepting startup ownership".to_owned(),
        )
    }

    fn new(kind: EngineStartErrorKind, detail: String) -> Self {
        Self { kind, detail }
    }
}

impl fmt::Display for EngineStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for EngineStartError {}

/// Failure while joining the explicit terminal engine shutdown.
#[derive(Debug)]
pub struct EngineShutdownError {
    detail: String,
}

impl EngineShutdownError {
    pub(crate) fn host(error: &EngineHostError) -> Self {
        Self {
            detail: error.to_string(),
        }
    }

    pub(crate) fn lock_poisoned() -> Self {
        Self {
            detail: "engine shutdown ownership lock is poisoned".to_owned(),
        }
    }
}

impl fmt::Display for EngineShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for EngineShutdownError {}

#[derive(Debug)]
pub(crate) enum EngineHostError {
    Clock(ClockError),
    Producer(ProducerHostInvariantError),
    ProducerStop(ProducerExecutionStopError),
    ProducerLockPoisoned,
    Completion(CompletionRegistryError),
    Driver(DriverOwnerError),
    DriverStopped,
    HostPanicked,
    Notifier(NotifierJoinError),
    Recovery {
        primary: Box<EngineHostError>,
        cleanup: Box<EngineHostError>,
    },
    #[cfg(test)]
    ForcedTestFailure,
}

impl fmt::Display for EngineHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(formatter, "engine clock failed: {error}"),
            Self::Producer(error) => write!(formatter, "producer host failed: {error}"),
            Self::ProducerStop(error) => write!(formatter, "producer recovery failed: {error}"),
            Self::ProducerLockPoisoned => {
                formatter.write_str("producer host ownership lock is poisoned")
            }
            Self::Completion(error) => {
                write!(formatter, "producer completion shutdown failed: {error}")
            }
            Self::Driver(error) => write!(formatter, "embedded driver failed: {error}"),
            Self::DriverStopped => formatter.write_str("embedded driver stopped unexpectedly"),
            Self::HostPanicked => formatter.write_str("engine host thread panicked"),
            Self::Notifier(error) => write!(formatter, "completion notifier failed: {error}"),
            Self::Recovery { primary, cleanup } => {
                write!(
                    formatter,
                    "{primary}; terminal cleanup also failed: {cleanup}"
                )
            }
            #[cfg(test)]
            Self::ForcedTestFailure => formatter.write_str("forced engine host test failure"),
        }
    }
}

impl EngineHostError {
    pub(super) fn with_cleanup(self, cleanup: Self) -> Self {
        Self::Recovery {
            primary: Box::new(self),
            cleanup: Box::new(cleanup),
        }
    }
}

impl std::error::Error for EngineHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Recovery { primary, .. } => Some(primary),
            _ => None,
        }
    }
}
