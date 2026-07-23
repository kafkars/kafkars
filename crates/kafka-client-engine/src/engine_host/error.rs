//! Stable startup failures and internal terminal host diagnostics.

use std::fmt;

use crate::{
    admin::CreateTopicsHostError,
    clock::ClockError,
    completion::NotifierJoinError,
    config::EngineConfigError,
    driver::{CreateTopicsCompletionFailure, DriverOwnerError, ProduceCompletionFailure},
    producer::{
        ProducerHostInvariantError, ProducerHostStartError, execution::PreparedProduceHandoffError,
        execution_stop::ProducerExecutionStopError, ingress::ProducerShardTerminalError,
    },
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
    /// Bounded admin resources could not be acquired.
    Admin,
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

    pub(super) fn admin(error: &std::io::Error) -> Self {
        Self::new(
            EngineStartErrorKind::Admin,
            format!("failed to start CreateTopics completion notifier: {error}"),
        )
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

/// Retained failure from explicit terminal engine shutdown.
#[derive(Clone, Debug)]
pub struct EngineShutdownError {
    kind: EngineShutdownErrorKind,
    detail: String,
}

impl EngineShutdownError {
    pub(crate) fn host(error: &EngineHostError) -> Self {
        Self {
            kind: EngineShutdownErrorKind::Host,
            detail: error.to_string(),
        }
    }

    pub(crate) fn notifier_thread() -> Self {
        Self {
            kind: EngineShutdownErrorKind::NotifierThread,
            detail: "shutdown was requested from the completion notifier; terminal cleanup \
                     continues after the callback returns"
                .to_owned(),
        }
    }

    /// Returns the stable shutdown-failure category.
    pub const fn kind(&self) -> EngineShutdownErrorKind {
        self.kind
    }
}

impl fmt::Display for EngineShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for EngineShutdownError {}

/// Stable category for an engine shutdown report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineShutdownErrorKind {
    /// Host or cleanup execution failed before terminal shutdown.
    Host,
    /// A notifier callback initiated shutdown and cannot wait for itself.
    NotifierThread,
}

#[derive(Debug)]
pub(crate) enum EngineHostError {
    Clock(ClockError),
    Producer(ProducerHostInvariantError),
    ProducerHandoff(PreparedProduceHandoffError),
    ProduceCompletion(ProduceCompletionFailure),
    ProducerStop(ProducerExecutionStopError),
    ProducerCleanup(ProducerShardTerminalError),
    ProducerLockPoisoned,
    Admin(CreateTopicsHostError),
    CreateTopicsCompletion(CreateTopicsCompletionFailure),
    AdminLockPoisoned,
    Driver(DriverOwnerError),
    DriverOwnerMissing,
    DriverStopped,
    TrackedProduceCallsRemain(usize),
    TrackedCreateTopicsCallsRemain(usize),
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
            Self::ProducerHandoff(error) => {
                write!(formatter, "prepared Produce handoff failed: {error}")
            }
            Self::ProduceCompletion(error) => write!(formatter, "{error}"),
            Self::ProducerStop(error) => write!(formatter, "producer recovery failed: {error}"),
            Self::ProducerCleanup(error) => {
                write!(formatter, "producer terminal cleanup failed: {error}")
            }
            Self::ProducerLockPoisoned => {
                formatter.write_str("producer host ownership lock is poisoned")
            }
            Self::Admin(error) => write!(formatter, "CreateTopics host failed: {error}"),
            Self::CreateTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::AdminLockPoisoned => {
                formatter.write_str("CreateTopics host ownership lock is poisoned")
            }
            Self::Driver(error) => write!(formatter, "embedded driver failed: {error}"),
            Self::DriverOwnerMissing => formatter.write_str("embedded driver owner is unavailable"),
            Self::DriverStopped => formatter.write_str("embedded driver stopped unexpectedly"),
            Self::TrackedProduceCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked Produce calls remain at terminal cleanup"
                )
            }
            Self::TrackedCreateTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked CreateTopics calls remain at terminal cleanup"
                )
            }
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
