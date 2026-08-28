//! Atomic join from a fresh route snapshot into exact-broker driver ownership.

mod failure;
mod submit;

use kafka_client_core::{
    Moment, ProducerAttemptFailureKind, partitioning::TopicMetadataGeneration,
};

use crate::{
    driver::{DriverOwner, TrackedProduceCalls},
    producer::{
        execution::{PreparedProduceHandoffError, PreparedProduceRouteKey},
        ingress::ProducerShardData,
    },
};

use super::{
    super::{EngineHostError, ProduceAdmissionOutcome},
    ProducerRoutingCall, RoutingStartPoll,
    resolution::{RoutedProduceGroup, first_available_broker_group, route_candidates},
};

use failure::{
    clear, key_is_current, reject_candidates, reject_submissions, settle_retained_failure,
};
use submit::submit_group;

pub(in crate::engine_host) fn admit(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    retained: &mut Option<ProducerRoutingCall>,
    data: &mut ProducerShardData,
    now: Moment,
    prepared_batch_budget: usize,
) -> Result<Option<ProduceAdmissionOutcome>, EngineHostError> {
    let Some(call) = retained.as_mut() else {
        return Ok(None);
    };
    if !key_is_current(call, data) {
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
    }
    if call
        .deadline()
        .is_some_and(|deadline| deadline.core().is_elapsed_at(now))
    {
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
    }
    match call.try_start(driver) {
        RoutingStartPoll::Submitted => {
            return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
        }
        RoutingStartPoll::Failed => {
            let transitioned = settle_retained_failure(retained, data, now, prepared_batch_budget)?;
            return Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)));
        }
        RoutingStartPoll::Pending => {}
    }
    if call.failure().is_some() {
        let transitioned = settle_retained_failure(retained, data, now, prepared_batch_budget)?;
        return Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)));
    }
    let Some(window) = data
        .next_produce_route_window(prepared_batch_budget)
        .map_err(EngineHostError::ProducerHandoff)?
    else {
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
    };
    let (key, candidates) = window.into_parts();
    let ready_matches = call
        .ready()
        .is_some_and(|(retained_key, _view)| retained_key == &key);
    if !ready_matches {
        if call.ready().is_some() {
            clear(retained);
            return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
        }
        return Ok(Some(ProduceAdmissionOutcome::idle()));
    }
    if key.deadline().core().is_elapsed_at(now) {
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
    }
    let (view_generation, routed) = {
        let (_retained_key, view) = call
            .ready()
            .unwrap_or_else(|| unreachable!("matching Produce topic view remains ready"));
        (
            view.metadata_generation(),
            route_candidates(candidates, &key, view),
        )
    };
    let mut groups = match routed {
        Ok(groups) => groups,
        Err((candidates, failure)) => {
            let transitioned = candidates.len();
            reject_candidates(data, candidates, now, failure)?;
            clear(retained);
            return Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)));
        }
    };
    let Some(index) = first_available_broker_group(
        groups.iter().map(RoutedProduceGroup::broker_id),
        |broker_id| calls.broker_admission_available(broker_id),
    ) else {
        return Ok(Some(ProduceAdmissionOutcome::idle()));
    };
    let group = groups.swap_remove(index);
    admit_group(
        driver,
        calls,
        retained,
        data,
        now,
        (&key, view_generation),
        group,
    )
}

fn admit_group(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    retained: &mut Option<ProducerRoutingCall>,
    data: &mut ProducerShardData,
    now: Moment,
    route: (&PreparedProduceRouteKey, TopicMetadataGeneration),
    group: RoutedProduceGroup,
) -> Result<Option<ProduceAdmissionOutcome>, EngineHostError> {
    let (key, view_generation) = route;
    let broker_id = group.broker_id();
    let accepted_in_flight_requests = calls.in_flight_request_count().saturating_add(1);
    let accepted_broker_in_flight_requests = calls
        .broker_in_flight_request_count(broker_id)
        .saturating_add(1);
    let Some(permit) = calls.try_reserve_for(broker_id) else {
        return Ok(Some(ProduceAdmissionOutcome::idle()));
    };
    let candidates = group.into_candidates();
    let transitioned = candidates.len();
    if !data
        .preflight_produce_route_identity(key, view_generation, &candidates)
        .map_err(EngineHostError::Producer)?
    {
        drop(permit);
        reject_candidates(
            data,
            candidates,
            now,
            ProducerAttemptFailureKind::RouteUnavailable,
        )?;
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)));
    }
    let mut submissions = match data.take_routed_produce_submissions(key, &candidates) {
        Ok(submissions) => submissions,
        Err(
            PreparedProduceHandoffError::RouteSnapshotMismatch { .. }
            | PreparedProduceHandoffError::OwnershipMismatch { .. },
        ) => {
            drop(permit);
            clear(retained);
            return Ok(Some(ProduceAdmissionOutcome::progressed(0)));
        }
        Err(error) => return Err(EngineHostError::ProducerHandoff(error)),
    };
    if !data
        .finalize_produce_route_identity(key, view_generation, &mut submissions)
        .map_err(EngineHostError::Producer)?
    {
        drop(permit);
        reject_submissions(
            data,
            &submissions,
            now,
            ProducerAttemptFailureKind::RouteUnavailable,
        )?;
        clear(retained);
        return Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)));
    }
    submit_group(
        driver,
        permit,
        data,
        submissions,
        now,
        accepted_in_flight_requests,
        accepted_broker_in_flight_requests,
    )?;
    Ok(Some(ProduceAdmissionOutcome::progressed(transitioned)))
}

pub(in crate::engine_host) fn apply_ready(
    retained: &mut Option<ProducerRoutingCall>,
    data: &mut ProducerShardData,
    now: Moment,
    prepared_batch_budget: usize,
) -> Result<ProduceAdmissionOutcome, EngineHostError> {
    let Some(call) = retained.as_mut() else {
        return Ok(ProduceAdmissionOutcome::idle());
    };
    if !key_is_current(call, data)
        || call
            .deadline()
            .is_some_and(|deadline| deadline.core().is_elapsed_at(now))
    {
        clear(retained);
        return Ok(ProduceAdmissionOutcome::progressed(0));
    }
    let progress = call.poll();
    if call.failure().is_some() {
        if prepared_batch_budget == 0 {
            return Ok(if progress {
                ProduceAdmissionOutcome::progressed(0)
            } else {
                ProduceAdmissionOutcome::idle()
            });
        }
        let transitioned = settle_retained_failure(retained, data, now, prepared_batch_budget)?;
        return Ok(ProduceAdmissionOutcome::progressed(transitioned));
    }
    Ok(if progress {
        ProduceAdmissionOutcome::progressed(0)
    } else {
        ProduceAdmissionOutcome::idle()
    })
}

pub(in crate::engine_host) fn discard_after_driver_shutdown(
    retained: &mut Option<ProducerRoutingCall>,
) {
    if let Some(call) = retained.take() {
        call.discard_after_driver_shutdown();
    }
}
