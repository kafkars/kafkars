//! Single-owner execution shell around one embedded driver reactor.

use std::{fmt, time::Duration};

use kafka_driver::{BootstrapError, BootstrapLimits, BootstrapSet, Driver, Reactor, TurnOutcome};

use crate::EngineConfig;

use super::{DriverOwnerError, ReactorWake, ValidatedSecurity, endpoint, shutdown::DriverShutdown};

pub(crate) mod observation;

/// Driver-neutral outcome from one embedded reactor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverTurn {
    Idle,
    Progress { more_work: bool },
    Shutdown,
}

/// Unique engine ownership of one driver handle, reactor, and wake source.
///
/// This type deliberately has no `Clone` implementation. It does not expose the
/// driver's current relative-timeout request methods; reactor turns and
/// terminal shutdown remain single-owner actions.
pub(crate) struct DriverOwner {
    pub(super) driver: Driver,
    reactor: Reactor,
    wake: ReactorWake,
    shutdown: DriverShutdown,
}

impl DriverOwner {
    /// Acquires one embedded driver reactor for configured logical endpoints.
    #[cfg(test)]
    pub(crate) fn build(config: &EngineConfig) -> Result<Self, DriverOwnerError> {
        let security = config.security();
        let security = super::security::validate(security).map_err(DriverOwnerError::Security)?;
        Self::build_with_security(config, security)
    }

    /// Acquires a reactor while consuming security validated at host admission.
    pub(crate) fn build_with_security(
        config: &EngineConfig,
        security: ValidatedSecurity,
    ) -> Result<Self, DriverOwnerError> {
        let limits = BootstrapLimits::default();
        let limit = limits.max_endpoints().get();
        if config.bootstrap_servers().len() > limit {
            return Err(DriverOwnerError::Bootstrap(BootstrapError::Capacity {
                limit,
            }));
        }

        let mut endpoints = Vec::with_capacity(config.bootstrap_servers().len());
        for (index, value) in config.bootstrap_servers().iter().enumerate() {
            let endpoint = endpoint::parse(value)
                .map_err(|source| DriverOwnerError::Endpoint { index, source })?;
            endpoints.push(endpoint);
        }
        let bootstrap =
            BootstrapSet::try_from_iter(endpoints, limits).map_err(DriverOwnerError::Bootstrap)?;
        let builder = super::security::builder(bootstrap, security);
        let builder = match config.client_id() {
            Some(client_id) => builder.client_id(client_id),
            None => builder,
        };
        let (driver, reactor) = builder.build_reactor().map_err(DriverOwnerError::Build)?;
        let wake = ReactorWake::new(reactor.wake_handle());
        Ok(Self {
            driver,
            reactor,
            wake,
            shutdown: DriverShutdown::default(),
        })
    }

    /// Shares the domain-neutral wake and host-turn demand handshake.
    pub(crate) fn reactor_wake(&self) -> ReactorWake {
        self.wake.clone()
    }

    /// Clears demand that the integrated host is about to inspect.
    pub(crate) fn acknowledge_host_turn(&self) {
        self.wake.acknowledge_host_turn();
    }

    /// Reports demand published after the current host turn began.
    pub(crate) fn host_turn_requested(&self) -> bool {
        self.wake.host_turn_requested()
    }

    /// Shares one command-only handle for bounded operational observation.
    pub(crate) fn observation_handle(&self) -> observation::DriverObservationHandle {
        observation::DriverObservationHandle::new(self.driver.clone())
    }

    /// Drives one fairness-bounded embedded driver turn.
    #[allow(
        unreachable_patterns,
        reason = "the published driver RC exposes a non-exhaustive turn outcome while the reviewed path dependency is exhaustive"
    )]
    pub(crate) fn turn(&mut self, max_wait: Duration) -> Result<DriverTurn, DriverOwnerError> {
        let outcome = self
            .reactor
            .turn(max_wait)
            .map_err(DriverOwnerError::Reactor)?;
        self.shutdown
            .observe()
            .map_err(DriverOwnerError::ShutdownCompletion)?;
        let outcome = match outcome {
            TurnOutcome::Idle => DriverTurn::Idle,
            TurnOutcome::Progress { more_work, .. } => DriverTurn::Progress { more_work },
            TurnOutcome::Shutdown { .. } => DriverTurn::Shutdown,
            // A future outcome cannot be guessed without changing owner-state
            // semantics such as progress, wake scheduling, or shutdown.
            _ => return Err(DriverOwnerError::UnrecognizedTurnOutcome),
        };
        Ok(outcome)
    }

    /// Requests and retains the driver's shared explicit shutdown barrier.
    pub(crate) fn close_admission(&mut self) -> Result<(), DriverOwnerError> {
        self.shutdown
            .begin(&self.driver)
            .map_err(DriverOwnerError::ShutdownSubmit)
    }

    /// Drives the explicit shared shutdown barrier within a caller-provided bound.
    pub(crate) fn shutdown_with_turn_limit(
        &mut self,
        turn_limit: usize,
        max_wait: Duration,
    ) -> Result<usize, DriverOwnerError> {
        self.close_admission()?;
        let mut turns = 0;
        while !self.is_shutdown() && turns < turn_limit {
            let _outcome = self.turn(max_wait)?;
            turns += 1;
        }
        if self.is_shutdown() {
            Ok(turns)
        } else {
            Err(DriverOwnerError::ShutdownTurnExhausted)
        }
    }

    /// Reports whether both reactor and retained shared barrier are terminal.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.reactor.is_shutdown() && self.shutdown.is_settled()
    }
}

impl fmt::Debug for DriverOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriverOwner")
            .field("shutdown_started", &self.shutdown.is_started())
            .field("shutdown_settled", &self.shutdown.is_settled())
            .field("reactor_shutdown", &self.reactor.is_shutdown())
            .finish_non_exhaustive()
    }
}
