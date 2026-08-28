//! Fair reactor-native execution and ordered terminal shutdown.

mod resources;

use crate::driver::DriverTurn;

use super::{
    EngineHostError, admin, assigned_consumer, cleanup, group_consumer,
    notifier_shutdown::NotifierShutdownOwner, produce_turn, share_consumer, transaction, wait,
};

pub(crate) use resources::EngineHostResources;
pub(crate) struct EngineHostExit {
    pub(super) notifier: NotifierShutdownOwner,
    pub(super) failure: Option<EngineHostError>,
}

#[derive(Default)]
pub(super) struct HostTurnState {
    driver_more_work: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag preserves one independently scheduled lane in the auditable turn report"
)]
pub(super) struct HostTurnProgress {
    pub(super) should_terminate: bool,
    pub(super) producer_admissions: usize,
    pub(super) producer_unsettled: usize,
    pub(super) admin_progressed: bool,
    pub(super) assigned_consumer_progressed: bool,
    pub(super) group_consumer_progressed: bool,
    pub(super) share_consumer_progressed: bool,
    pub(super) transaction_progressed: bool,
    pub(super) driver_turned: bool,
    pub(super) producer_completions_progressed: bool,
    pub(super) admin_completions_progressed: bool,
}

pub(crate) fn run(resources: &mut EngineHostResources) -> Result<EngineHostExit, EngineHostError> {
    let mut state = HostTurnState::default();
    loop {
        if drive_host_turn(resources, &mut state)?.should_terminate {
            break;
        }
    }

    cleanup::shutdown_driver(resources)?;
    cleanup::prepare_notification_stop(resources)?;
    let (notifier, failure) = cleanup::begin_notification_shutdown(resources)?;
    Ok(EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifier),
        failure,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "one host turn keeps the fairness order and every progress fact auditable"
)]
pub(super) fn drive_host_turn(
    resources: &mut EngineHostResources,
    state: &mut HostTurnState,
) -> Result<HostTurnProgress, EngineHostError> {
    resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?
        .acknowledge_host_turn();
    #[cfg(test)]
    if resources.control.failure_requested() {
        return Err(EngineHostError::ForcedTestFailure);
    }
    let producer_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let producer = produce_turn::drive(resources, producer_now)?;
    // Producer execution may encode and submit before admin receives its turn.
    // Recapture time so admin never derives timeouts from a stale observation.
    let admin = admin::drive(resources)?;
    let assigned_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let assigned = assigned_consumer::drive(resources, assigned_now)?;
    let group_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let group = group_consumer::drive(resources, group_now)?;
    let share_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let share = share_consumer::drive(resources, share_now)?;
    let transaction_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let transaction = transaction::drive(resources, transaction_now)?;
    #[cfg(test)]
    if producer.driver_progress && resources.control.await_failure_after_produce_admission() {
        return Err(EngineHostError::ForcedTestFailure);
    }
    let should_terminate = resources.control.shutdown_requested()
        && producer.unsettled == 0
        && admin.unsettled == 0
        && assigned.unsettled == 0
        && assigned.close_completed
        && group.unsettled == 0
        && share.unsettled == 0
        && transaction.unsettled == 0;
    let mut progress = HostTurnProgress {
        should_terminate,
        producer_admissions: producer.admissions,
        producer_unsettled: producer.unsettled,
        admin_progressed: admin.driver_progress,
        assigned_consumer_progressed: assigned.progressed,
        group_consumer_progressed: group.progressed,
        share_consumer_progressed: share.progressed,
        transaction_progressed: transaction.progressed,
        driver_turned: false,
        producer_completions_progressed: false,
        admin_completions_progressed: false,
    };
    if should_terminate {
        return Ok(progress);
    }
    let wait_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let wait = wait::producer(
        wait_now,
        producer.outcome,
        state.driver_more_work
            || producer.driver_progress
            || admin.driver_progress
            || transaction.progressed,
    );
    let wait = admin.next_deadline.map_or(wait, |deadline| {
        wait.min(wait::deadline(wait_now, deadline))
    });
    let wait = wait::assigned_consumer(wait_now, wait, &assigned);
    let wait = wait::group_consumer(wait_now, wait, &group);
    let wait = wait::share_consumer(wait_now, wait, &share);
    let wait = transaction.next_deadline.map_or(wait, |deadline| {
        wait.min(wait::deadline(wait_now, deadline))
    });
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let wait = wait::host_turn(wait, driver.host_turn_requested());
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
    progress.driver_turned = true;
    let completion_now = resources.clock.now().map_err(EngineHostError::Clock)?;
    let completion_progress = produce_turn::apply_completions(
        driver,
        &resources.producer,
        &mut resources.producer_identity_calls,
        &mut resources.producer_partitioning_call,
        &mut resources.producer_routing_call,
        &mut resources.produce_calls,
        completion_now,
        producer.remaining_admission_budget(),
    )?;
    let admin_completion_progress = admin::apply_completions(resources)?;
    progress.producer_admissions = progress
        .producer_admissions
        .saturating_add(completion_progress.prepared_batches);
    progress.producer_completions_progressed = completion_progress.progressed;
    progress.admin_completions_progressed = admin_completion_progress;
    state.driver_more_work =
        driver_turn_more || completion_progress.progressed || admin_completion_progress;
    Ok(progress)
}
