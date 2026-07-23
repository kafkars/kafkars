//! Single-owner execution shell around one embedded driver reactor.

use std::{fmt, time::Duration};

use kafka_driver::{
    BootstrapError, BootstrapLimits, BootstrapSet, Call, Driver, Reactor, TurnOutcome, WakeHandle,
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

/// One bounded attempt to enter terminal driver shutdown.
pub(crate) enum DriverShutdownStart {
    Started(DriverShutdown),
    Retry,
    AlreadyShutdown,
}

/// Sole terminal barrier retained while the engine drives driver shutdown.
pub(crate) struct DriverShutdown {
    call: Call<()>,
}

impl DriverShutdown {
    pub(crate) fn wait(self) -> Result<(), DriverOwnerError> {
        self.call
            .wait()
            .map_err(DriverOwnerError::ShutdownCompletion)
    }
}

/// Unique engine ownership of one driver handle, reactor, and wake source.
///
/// This type deliberately has no `Clone` implementation. It does not expose the
/// driver's current relative-timeout request methods; reactor turns and
/// terminal shutdown remain single-owner actions.
pub(crate) struct DriverOwner {
    driver: Driver,
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
            driver,
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

    /// Enters the driver's priority shutdown lane and returns its barrier.
    pub(crate) fn begin_shutdown(&self) -> Result<DriverShutdownStart, DriverOwnerError> {
        match self.driver.shutdown() {
            Ok(call) => Ok(DriverShutdownStart::Started(DriverShutdown { call })),
            Err(kafka_driver::SubmitError::Full | kafka_driver::SubmitError::Wake(_)) => {
                Ok(DriverShutdownStart::Retry)
            }
            Err(kafka_driver::SubmitError::Closed) if self.reactor.is_shutdown() => {
                Ok(DriverShutdownStart::AlreadyShutdown)
            }
            Err(kafka_driver::SubmitError::Closed) => Err(DriverOwnerError::ShutdownClosed),
            Err(kafka_driver::SubmitError::IdentityExhausted) => {
                Err(DriverOwnerError::ShutdownIdentityExhausted)
            }
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
            .field("reactor_shutdown", &self.reactor.is_shutdown())
            .finish_non_exhaustive()
    }
}
