//! Fair reactor-native execution and ordered terminal shutdown.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::MonotonicClock,
    completion::NotifierJoin,
    driver::{DriverOwner, DriverShutdownStart, DriverTurn},
    producer::{
        ProducerTurnBudget, ProducerTurnOutcome,
        ingress::{ProducerShardLockError, ProducerShardOwner},
    },
};

use super::{EngineHostControl, EngineHostError};

// Admission and shutdown report wake failure without revoking ownership. Until
// that path can synchronously terminalize, this cap preserves deadline and
// shutdown liveness after an operating-system wake failure.
const HOST_PARK_LIMIT: Duration = Duration::from_millis(100);
const BLOCKED_RETRY_DELAY: Duration = HOST_PARK_LIMIT;
const SHUTDOWN_ADMISSION_ATTEMPTS: usize = 16;

pub(crate) struct EngineHostResources {
    pub(super) driver: DriverOwner,
    pub(super) producer: ProducerShardOwner,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) control: Arc<EngineHostControl>,
    pub(super) budget: ProducerTurnBudget,
}

impl Drop for EngineHostResources {
    fn drop(&mut self) {
        let _close_result = self.producer.close_admission();
    }
}

pub(crate) struct EngineHostExit {
    pub(super) notifier: NotifierJoin,
    pub(super) failure: Option<EngineHostError>,
}

pub(crate) fn run(resources: &mut EngineHostResources) -> Result<EngineHostExit, EngineHostError> {
    let mut driver_more_work = false;
    loop {
        #[cfg(test)]
        if resources.control.failure_requested() {
            return Err(EngineHostError::ForcedTestFailure);
        }
        let now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let producer = drive_producer(resources, now)?;
        if resources.control.shutdown_requested() && producer.unsettled == 0 {
            break;
        }
        let wait = producer_wait(now, producer.outcome, driver_more_work);
        resources.control.record_driver_turn();
        driver_more_work = match resources
            .driver
            .turn(wait)
            .map_err(EngineHostError::Driver)?
        {
            DriverTurn::Idle => false,
            DriverTurn::Progress { more_work } => more_work,
            DriverTurn::Shutdown => return Err(EngineHostError::DriverStopped),
        };
    }

    shutdown_driver(resources)?;
    let notifier = stop_notifier(resources)?;
    Ok(EngineHostExit {
        notifier,
        failure: None,
    })
}

pub(super) fn shutdown_driver(resources: &mut EngineHostResources) -> Result<(), EngineHostError> {
    let mut barrier = None;
    for _attempt in 0..SHUTDOWN_ADMISSION_ATTEMPTS {
        match resources
            .driver
            .begin_shutdown()
            .map_err(EngineHostError::Driver)?
        {
            DriverShutdownStart::Started(value) => {
                barrier = Some(value);
                break;
            }
            DriverShutdownStart::AlreadyShutdown => break,
            DriverShutdownStart::Retry => {
                resources.control.record_driver_turn();
                let _outcome = resources
                    .driver
                    .turn(Duration::ZERO)
                    .map_err(EngineHostError::Driver)?;
            }
        }
    }
    if barrier.is_none() && !resources.driver.is_shutdown() {
        return Err(EngineHostError::Driver(
            crate::driver::DriverOwnerError::ShutdownRetryExhausted,
        ));
    }
    while !resources.driver.is_shutdown() {
        resources.control.record_driver_turn();
        let _outcome = resources
            .driver
            .turn(HOST_PARK_LIMIT)
            .map_err(EngineHostError::Driver)?;
    }
    if let Some(barrier) = barrier {
        barrier.wait().map_err(EngineHostError::Driver)?;
    }
    Ok(())
}

struct ProducerProgress {
    outcome: Option<ProducerTurnOutcome>,
    unsettled: usize,
}

fn drive_producer(
    resources: &EngineHostResources,
    now: Moment,
) -> Result<ProducerProgress, EngineHostError> {
    let mut host = match resources.producer.try_host() {
        Ok(host) => host,
        Err(ProducerShardLockError::Contended) => {
            return Ok(ProducerProgress {
                outcome: None,
                unsettled: usize::MAX,
            });
        }
        Err(ProducerShardLockError::Poisoned) => {
            return Err(EngineHostError::ProducerLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        host.close_admission();
    }
    resources.control.record_producer_turn();
    let outcome = host
        .turn(now, resources.budget)
        .map_err(EngineHostError::Producer)?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: host.unsettled_completions(),
    })
}

pub(super) fn producer_wait(
    now: Moment,
    outcome: Option<ProducerTurnOutcome>,
    driver_more_work: bool,
) -> Duration {
    if driver_more_work {
        return Duration::ZERO;
    }
    let Some(outcome) = outcome else {
        return HOST_PARK_LIMIT;
    };
    if outcome.runnable_work {
        return Duration::ZERO;
    }
    let deadline_wait = outcome
        .next_deadline
        .map_or(HOST_PARK_LIMIT, |deadline| duration_until(now, deadline));
    if outcome.blocked_work {
        deadline_wait.min(BLOCKED_RETRY_DELAY)
    } else {
        deadline_wait
    }
}

fn duration_until(now: Moment, deadline: Deadline) -> Duration {
    Duration::from_nanos(deadline.tick().saturating_sub(now.tick()))
}

pub(super) fn stop_notifier(
    resources: &EngineHostResources,
) -> Result<NotifierJoin, EngineHostError> {
    let mut host = resources.producer.terminal_host();
    host.stop_notifier().map_err(EngineHostError::Completion)
}
