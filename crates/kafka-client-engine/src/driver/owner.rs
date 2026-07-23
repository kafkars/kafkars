//! Single-owner execution shell around one embedded driver reactor.

use std::{fmt, time::Duration};

use kafka_driver::{
    BootstrapError, BootstrapLimits, BootstrapSet, Driver, Reactor, TurnOutcome, WakeHandle,
};

use crate::EngineConfig;

use super::{DriverOwnerError, ProducerDriverWake, endpoint};

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
    driver: Option<Driver>,
    reactor: Reactor,
    wake: WakeHandle,
}

impl DriverOwner {
    /// Acquires one embedded driver reactor for configured logical endpoints.
    pub(crate) fn build(config: &EngineConfig) -> Result<Self, DriverOwnerError> {
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
        let (driver, reactor) = Driver::builder()
            .bootstrap(bootstrap)
            .build_reactor()
            .map_err(DriverOwnerError::Build)?;
        let wake = reactor.wake_handle();
        Ok(Self {
            driver: Some(driver),
            reactor,
            wake,
        })
    }

    /// Shares an engine-owned producer adapter over the coalesced reactor wake.
    pub(crate) fn producer_wake(&self) -> ProducerDriverWake {
        ProducerDriverWake::new(self.wake.clone())
    }

    /// Drives one fairness-bounded embedded driver turn.
    pub(crate) fn turn(&mut self, max_wait: Duration) -> Result<DriverTurn, DriverOwnerError> {
        let outcome = self
            .reactor
            .turn(max_wait)
            .map_err(DriverOwnerError::Reactor)?;
        Ok(match outcome {
            TurnOutcome::Idle => DriverTurn::Idle,
            TurnOutcome::Progress { more_work, .. } => DriverTurn::Progress { more_work },
            TurnOutcome::Shutdown { .. } => DriverTurn::Shutdown,
        })
    }

    /// Drops the sole command sender so the reactor begins implicit shutdown.
    pub(crate) fn close_admission(&mut self) {
        drop(self.driver.take());
    }

    /// Drives implicit shutdown within one caller-provided turn bound.
    pub(crate) fn shutdown_with_turn_limit(
        &mut self,
        turn_limit: usize,
        max_wait: Duration,
    ) -> Result<usize, DriverOwnerError> {
        self.close_admission();
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

    /// Reports whether the embedded reactor has reached terminal shutdown.
    pub(crate) const fn is_shutdown(&self) -> bool {
        self.reactor.is_shutdown()
    }
}

impl fmt::Debug for DriverOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriverOwner")
            .field("admission_open", &self.driver.is_some())
            .field("reactor_shutdown", &self.reactor.is_shutdown())
            .finish_non_exhaustive()
    }
}
