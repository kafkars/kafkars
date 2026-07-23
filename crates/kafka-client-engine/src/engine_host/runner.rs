//! Fair reactor-native execution and ordered terminal shutdown.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::CreateTopicsShardOwner,
    clock::MonotonicClock,
    completion::NotifierJoin,
    driver::{DriverOwner, DriverTurn, TrackedCreateTopicsCalls, TrackedProduceCalls},
    producer::{
        host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
        ingress::ProducerShardOwner,
    },
};

use super::{
    EngineHostControl, EngineHostError, admin,
    notifier_shutdown::{NotifierShutdownOwner, collect_notification_joins},
    produce_turn,
};

// Admission and shutdown report wake failure without revoking ownership. Until
// that path can synchronously terminalize, this cap preserves deadline and
// shutdown liveness after an operating-system wake failure.
const HOST_PARK_LIMIT: Duration = Duration::from_millis(100);
const BLOCKED_RETRY_DELAY: Duration = HOST_PARK_LIMIT;
const SHUTDOWN_TURN_ATTEMPTS: usize = 64;

pub(crate) struct EngineHostResources {
    pub(super) driver: Option<DriverOwner>,
    pub(super) producer: ProducerShardOwner,
    pub(super) admin: CreateTopicsShardOwner,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) control: Arc<EngineHostControl>,
    pub(super) budget: ProducerTurnBudget,
    pub(super) produce_calls: TrackedProduceCalls,
    pub(super) create_topics_calls: TrackedCreateTopicsCalls,
}

impl Drop for EngineHostResources {
    fn drop(&mut self) {
        let _close_result = self.producer.close_admission();
        let _close_result = self.admin.admission_port().close_admission();
    }
}

pub(crate) struct EngineHostExit {
    pub(super) notifier: NotifierShutdownOwner,
    pub(super) failure: Option<EngineHostError>,
}

pub(crate) fn run(resources: &mut EngineHostResources) -> Result<EngineHostExit, EngineHostError> {
    let mut driver_more_work = false;
    loop {
        #[cfg(test)]
        if resources.control.failure_requested() {
            return Err(EngineHostError::ForcedTestFailure);
        }
        let producer_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let producer = produce_turn::drive(resources, producer_now)?;
        // Producer execution may encode, submit, and apply completions before
        // admin receives its turn. Recapture time so CreateTopics never
        // computes a broker timeout from a stale pre-producer observation.
        let admin_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let admin = admin::drive(resources, admin_now)?;
        #[cfg(test)]
        if producer.driver_progress && resources.control.await_failure_after_produce_admission() {
            return Err(EngineHostError::ForcedTestFailure);
        }
        if resources.control.shutdown_requested() && producer.unsettled == 0 && admin.unsettled == 0
        {
            break;
        }
        let wait = producer_wait(
            admin_now,
            producer.outcome,
            driver_more_work || producer.driver_progress || admin.driver_progress,
        );
        let wait = admin.next_deadline.map_or(wait, |deadline| {
            wait.min(duration_until(admin_now, deadline))
        });
        resources.control.record_driver_turn();
        let driver = resources
            .driver
            .as_mut()
            .ok_or(EngineHostError::DriverOwnerMissing)?;
        let driver_turn_more = match driver.turn(wait).map_err(EngineHostError::Driver)? {
            DriverTurn::Idle => false,
            DriverTurn::Progress { more_work } => more_work,
            DriverTurn::Shutdown => return Err(EngineHostError::DriverStopped),
        };
        let completion_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let completion_progress = produce_turn::apply_completions(
            &resources.producer,
            &mut resources.produce_calls,
            completion_now,
        )?;
        let admin_completion_progress = admin::apply_completions(resources)?;
        driver_more_work = driver_turn_more || completion_progress || admin_completion_progress;
    }

    shutdown_driver(resources)?;
    prepare_notification_stop(resources)?;
    let (notifier, failure) = begin_notification_shutdown(resources)?;
    Ok(EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifier),
        failure,
    })
}

fn begin_notification_shutdown(
    resources: &EngineHostResources,
) -> Result<(Vec<NotifierJoin>, Option<EngineHostError>), EngineHostError> {
    let mut data = resources.producer.terminal_data();
    let producer = data
        .begin_notification_shutdown()
        .map_err(EngineHostError::ProducerCleanup)?;
    drop(data);
    let mut admin_host = resources.admin.terminal_host();
    let admin = admin_host.stop_notifier().map_err(EngineHostError::Admin);
    let fallback = admin_host.recover_notifier();
    drop(admin_host);
    Ok(collect_notification_joins(producer, admin, fallback))
}

pub(super) fn shutdown_driver(resources: &mut EngineHostResources) -> Result<(), EngineHostError> {
    let driver = resources
        .driver
        .as_mut()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turns = driver
        .shutdown_with_turn_limit(SHUTDOWN_TURN_ATTEMPTS, HOST_PARK_LIMIT)
        .map_err(EngineHostError::Driver)?;
    for _turn in 0..turns {
        resources.control.record_driver_turn();
    }
    Ok(())
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

/// Verifies all producer-owned admission and terminal work before the
/// completion notifier is stopped.
pub(super) fn prepare_notification_stop(
    resources: &EngineHostResources,
) -> Result<(), EngineHostError> {
    let tracked_calls = resources.produce_calls.retained_count();
    if tracked_calls != 0 {
        return Err(EngineHostError::TrackedProduceCallsRemain(tracked_calls));
    }
    let tracked_admin_calls = resources.create_topics_calls.retained_count();
    if tracked_admin_calls != 0 {
        return Err(EngineHostError::TrackedCreateTopicsCallsRemain(
            tracked_admin_calls,
        ));
    }
    let admin_unsettled = resources.admin.terminal_host().unsettled();
    if admin_unsettled != 0 {
        return Err(EngineHostError::Admin(
            crate::admin::CreateTopicsHostError::Unsettled(admin_unsettled),
        ));
    }
    let mut data = resources.producer.terminal_data();
    let release = data.verify_release_before_completion();
    let failure = release.err().map(EngineHostError::ProducerCleanup);
    data.drain_terminal_mechanisms();
    let final_failure = data
        .verify_terminal_cleanup()
        .err()
        .map(EngineHostError::ProducerCleanup);
    if let Some(error) = combine_cleanup(failure, final_failure) {
        return Err(error);
    }
    Ok(())
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
