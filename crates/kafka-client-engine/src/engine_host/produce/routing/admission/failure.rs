//! Fresh-correlation handling for routing failures and stale retained views.

use kafka_client_core::{Moment, ProducerAttemptFailureKind};

use crate::producer::{
    execution::{PreparedProduceRouteCandidate, PreparedProduceSubmission},
    ingress::ProducerShardData,
};

use super::super::super::{EngineHostError, reject_execution};
use super::super::{ProducerRoutingCall, RoutingFailure};

pub(super) fn settle_retained_failure(
    retained: &mut Option<ProducerRoutingCall>,
    data: &mut ProducerShardData,
    now: Moment,
    prepared_batch_budget: usize,
) -> Result<usize, EngineHostError> {
    let Some(call) = retained.as_ref() else {
        return Ok(0);
    };
    if !key_is_current(call, data) {
        clear(retained);
        return Ok(0);
    }
    let mut transitioned = 0;
    let failure = call.failure();
    if let Some(RoutingFailure::Attempt(failure)) = failure {
        let Some(window) = data
            .next_produce_route_window(prepared_batch_budget)
            .map_err(EngineHostError::ProducerHandoff)?
        else {
            clear(retained);
            return Ok(0);
        };
        if call.key() == Some(window.key()) {
            let (_key, candidates) = window.into_parts();
            transitioned = candidates.len();
            reject_candidates(data, candidates, now, failure)?;
        }
    }
    clear(retained);
    Ok(transitioned)
}

pub(super) fn reject_candidates(
    data: &mut ProducerShardData,
    candidates: Vec<PreparedProduceRouteCandidate>,
    now: Moment,
    failure: ProducerAttemptFailureKind,
) -> Result<(), EngineHostError> {
    for candidate in candidates {
        reject_execution(data, candidate.execution(), now, failure)?;
    }
    Ok(())
}

pub(super) fn reject_submissions(
    data: &mut ProducerShardData,
    submissions: &[PreparedProduceSubmission],
    now: Moment,
    failure: ProducerAttemptFailureKind,
) -> Result<(), EngineHostError> {
    for submission in submissions {
        reject_execution(data, submission.execution(), now, failure)?;
    }
    Ok(())
}

pub(super) fn key_is_current(call: &ProducerRoutingCall, data: &ProducerShardData) -> bool {
    data.next_produce_route_key().as_ref() == call.key()
}

pub(super) fn clear(retained: &mut Option<ProducerRoutingCall>) {
    if let Some(call) = retained.as_mut() {
        call.abandon();
    }
    *retained = None;
}
