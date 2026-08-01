//! Single-owner execution shell around one embedded driver reactor.

use std::{fmt, time::Duration};

use kafka_driver::{
    BootstrapError, BootstrapLimits, BootstrapSet, Driver, Reactor, TurnOutcome, WakeHandle,
};

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
    wake: WakeHandle,
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
        // The reviewed driver does not yet expose request-header client-ID
        // configuration. Preserve the engine setting for a later driver bridge.
        let _configured_client_id = config.client_id();
        let (driver, reactor) = builder.build_reactor().map_err(DriverOwnerError::Build)?;
        let wake = reactor.wake_handle();
        Ok(Self {
            driver,
            reactor,
            wake,
            shutdown: DriverShutdown::default(),
        })
    }

    /// Shares the domain-neutral coalescing wake for the integrated host.
    pub(crate) fn reactor_wake(&self) -> ReactorWake {
        ReactorWake::new(self.wake.clone())
    }

    /// Shares one command-only handle for bounded operational observation.
    pub(crate) fn observation_handle(&self) -> observation::DriverObservationHandle {
        observation::DriverObservationHandle::new(self.driver.clone())
    }

    /// Drives one fairness-bounded embedded driver turn.
    pub(crate) fn turn(&mut self, max_wait: Duration) -> Result<DriverTurn, DriverOwnerError> {
        let outcome = self
            .reactor
            .turn(max_wait)
            .map_err(DriverOwnerError::Reactor)?;
        self.shutdown
            .observe()
            .map_err(DriverOwnerError::ShutdownCompletion)?;
        Ok(match outcome {
            TurnOutcome::Idle => DriverTurn::Idle,
            TurnOutcome::Progress { more_work, .. } => DriverTurn::Progress { more_work },
            TurnOutcome::Shutdown { .. } => DriverTurn::Shutdown,
        })
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
