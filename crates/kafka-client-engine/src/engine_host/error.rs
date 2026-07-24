//! Stable startup failures and internal terminal host diagnostics.

use std::fmt;

use crate::{
    config::EngineConfigError, consumer::AssignedConsumerOwnerBuildError, driver::DriverOwnerError,
    producer::ProducerHostStartError,
};

mod host;
mod host_display;
#[cfg(test)]
mod host_display_test;
#[cfg(test)]
mod host_test;

pub(crate) use host::EngineHostError;

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
    /// Bounded direct-consumer resources could not be acquired.
    Consumer,
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

    pub(super) fn admin_notifier(error: &std::io::Error) -> Self {
        Self::new(
            EngineStartErrorKind::Admin,
            format!("failed to start shared admin completion notifier: {error}"),
        )
    }

    pub(super) fn assigned_consumer(error: AssignedConsumerOwnerBuildError) -> Self {
        Self::new(
            EngineStartErrorKind::Consumer,
            format!("failed to start assigned consumer: {error:?}"),
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
