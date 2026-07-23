//! Fair reactor-native execution and ordered terminal shutdown.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::MonotonicClock,
    completion::NotifierJoin,
    driver::{DriverOwner, DriverTurn},
    producer::{
        host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
        ingress::{ProducerShardLockError, ProducerShardOwner},
    },
};

use super::{EngineHostControl, EngineHostError};

// Admission and shutdown report wake failure without revoking ownership. Until
// that path can synchronously terminalize, this cap preserves deadline and
// shutdown liveness after an operating-system wake failure.
const HOST_PARK_LIMIT: Duration = Duration::from_millis(100);
const BLOCKED_RETRY_DELAY: Duration = HOST_PARK_LIMIT;
const SHUTDOWN_TURN_ATTEMPTS: usize = 64;

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
    pub(super) notifier: Option<NotifierJoin>,
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
        notifier: Some(notifier),
        failure: None,
    })
}

pub(super) fn shutdown_driver(resources: &mut EngineHostResources) -> Result<(), EngineHostError> {
    let turns = resources
        .driver
        .shutdown_with_turn_limit(SHUTDOWN_TURN_ATTEMPTS, HOST_PARK_LIMIT)
        .map_err(EngineHostError::Driver)?;
    for _turn in 0..turns {
        resources.control.record_driver_turn();
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
    let mut data = match resources.producer.try_data() {
        Ok(data) => data,
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
        data.close_admission();
    }
    resources.control.record_producer_turn();
    let outcome = data
        .turn(now, resources.budget)
        .map_err(EngineHostError::Producer)?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: data.unsettled_completions(),
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
    let deadline_wait = outcome.next_deadline.map_or(HOST_PARK_LIMIT, |deadline| {
        duration_until(now, deadline).min(HOST_PARK_LIMIT)
    });
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
    let mut data = resources.producer.terminal_data();
    let release = data.verify_release_before_completion();
    if let Err(error) = release
        && error.pending_ownership().is_some()
    {
        return Err(EngineHostError::ProducerCleanup(error));
    }
    let mut failure = release.err().map(EngineHostError::ProducerCleanup);
    let drain_failure = data
        .drain_terminal_mechanisms()
        .err()
        .map(EngineHostError::ProducerCleanup);
    failure = combine_cleanup(failure, drain_failure);
    let final_failure = data
        .verify_terminal_cleanup()
        .err()
        .map(EngineHostError::ProducerCleanup);
    if let Some(error) = combine_cleanup(failure, final_failure) {
        return Err(error);
    }
    data.stop_notifier()
        .map_err(EngineHostError::ProducerCleanup)
}

fn combine_cleanup(
    primary: Option<EngineHostError>,
    cleanup: Option<EngineHostError>,
) -> Option<EngineHostError> {
    match (primary, cleanup) {
        (Some(primary), Some(cleanup)) => Some(primary.with_cleanup(cleanup)),
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    }
}
