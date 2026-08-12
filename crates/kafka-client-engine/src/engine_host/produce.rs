//! Atomic join between prepared producer ownership and tracked driver calls.

mod partitioning;

use kafka_client_core::{Moment, ProducerInput};

use crate::{
    driver::{
        DriverOwner, ProduceRouteRefreshPoll, TrackedProduceCalls, TrackedProducerIdentityCalls,
    },
    producer::ingress::ProducerShardData,
};

use super::EngineHostError;

pub(super) use partitioning::{
    ProducerPartitioningCall, admit as admit_partitioning, apply_ready as apply_partitioning_ready,
    discard_after_driver_shutdown as discard_partitioning_after_driver_shutdown,
};

/// Attempts the one lazy nontransactional identity acquisition.
pub(super) fn admit_identity(
    driver: &DriverOwner,
    calls: &mut TrackedProducerIdentityCalls,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<bool, EngineHostError> {
    let Some(permit) = calls.try_reserve() else {
        return Ok(false);
    };
    let Some(submission) = data
        .take_identity_submission()
        .map_err(EngineHostError::ProducerIdentityHandoff)?
    else {
        return Ok(false);
    };
    let (generation, deadline) = submission.into_parts();
    match permit.submit(driver, generation, deadline) {
        Ok(()) => Ok(true),
        Err(rejection) => {
            drop(rejection);
            data.apply_produce_driver_input(
                now,
                ProducerInput::ProducerIdentityRequestFailed { generation, now },
            )
            .map_err(EngineHostError::Producer)?;
            Ok(true)
        }
    }
}

/// Attempts one prepared admission under the producer shard's existing guard.
pub(super) fn admit_one(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<bool, EngineHostError> {
    if !calls.broker_admission_available(None) {
        return Ok(false);
    }
    let Some(permit) = calls.try_reserve() else {
        return Ok(false);
    };
    let mut submissions = data
        .take_produce_submissions()
        .map_err(EngineHostError::ProducerHandoff)?;
    if submissions.is_empty() {
        return Ok(false);
    }
    let request_batches = submissions.len();
    let request_records = submissions.iter().fold(0_u64, |total, submission| {
        total.saturating_add(u64::from(submission.record_count()))
    });
    let request_bytes = submissions.iter().fold(0_usize, |total, submission| {
        total.saturating_add(submission.encoded_record_bytes())
    });
    if submissions.len() > 1 {
        match permit.submit_batch(driver, submissions, now) {
            Ok(accepted) => {
                data.record_produce_request(
                    request_batches,
                    request_records,
                    request_bytes,
                    calls.in_flight_request_count(),
                    calls.max_broker_in_flight_request_count(),
                );
                for input in accepted.inputs() {
                    data.apply_produce_driver_input(now, input)
                        .map_err(EngineHostError::Producer)?;
                }
                accepted.confirm_receipt();
            }
            Err(rejection) => {
                debug_assert_eq!(
                    rejection.delivery(),
                    kafka_client_core::DeliveryStatus::NotSent
                );
                let failure = rejection.failure_kind();
                for execution in rejection.executions() {
                    data.apply_produce_driver_input(
                        now,
                        ProducerInput::DriverRejected {
                            execution,
                            now,
                            failure,
                        },
                    )
                    .map_err(EngineHostError::Producer)?;
                }
            }
        }
        return Ok(true);
    }
    let submission = submissions
        .pop()
        .unwrap_or_else(|| unreachable!("nonempty single Produce handoff"));
    let (execution, deadline, materialized) = submission.into_parts();
    match permit.submit(driver, execution, deadline, materialized, now) {
        Ok(accepted) => {
            data.record_produce_request(
                request_batches,
                request_records,
                request_bytes,
                calls.in_flight_request_count(),
                calls.max_broker_in_flight_request_count(),
            );
            debug_assert_eq!(accepted.execution(), execution);
            data.apply_produce_driver_input(now, accepted.driver_accepted())
                .map_err(EngineHostError::Producer)?;
            accepted.confirm_receipt();
        }
        Err(rejection) => {
            debug_assert_eq!(
                rejection.delivery(),
                kafka_client_core::DeliveryStatus::NotSent
            );
            let failure = rejection.failure_kind();
            drop(rejection);
            data.apply_produce_driver_input(
                now,
                ProducerInput::DriverRejected {
                    execution,
                    now,
                    failure,
                },
            )
            .map_err(EngineHostError::Producer)?;
        }
    }
    Ok(true)
}

/// Applies at most `budget` terminal driver facts under the shard guard.
pub(super) fn apply_ready(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    data: &mut ProducerShardData,
    now: Moment,
    budget: usize,
) -> Result<bool, EngineHostError> {
    let mut progress = false;
    for _attempt in 0..budget {
        let Some(settled) = calls
            .poll_next_ready(now)
            .map_err(EngineHostError::ProduceCompletion)?
        else {
            break;
        };
        match settled.poll_route_refresh(driver, now) {
            ProduceRouteRefreshPoll::Ready => {}
            ProduceRouteRefreshPoll::Failed => {}
            ProduceRouteRefreshPoll::Submitted => {
                progress = true;
                break;
            }
            ProduceRouteRefreshPoll::Pending => break,
        }
        data.apply_produce_driver_input(now, settled.input())
            .map_err(EngineHostError::Producer)?;
        calls.discard_settled(now);
        progress = true;
    }
    Ok(progress)
}

/// Applies at most one terminal identity fact under the producer shard guard.
pub(super) fn apply_identity_ready(
    calls: &mut TrackedProducerIdentityCalls,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<bool, EngineHostError> {
    let Some(input) = calls
        .poll_ready(now)
        .map_err(EngineHostError::ProducerIdentityCompletion)?
    else {
        return Ok(false);
    };
    data.apply_produce_driver_input(now, input)
        .map_err(EngineHostError::Producer)?;
    Ok(true)
}
