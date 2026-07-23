//! Atomic join between prepared producer ownership and tracked driver calls.

use kafka_client_core::{Moment, ProducerInput};

use crate::{
    driver::{DriverOwner, TrackedProduceCalls},
    producer::ingress::ProducerShardData,
};

use super::EngineHostError;

/// Attempts one prepared admission under the producer shard's existing guard.
pub(super) fn admit_one(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<bool, EngineHostError> {
    let Some(permit) = calls.try_reserve() else {
        return Ok(false);
    };
    let Some(submission) = data
        .take_produce_submission()
        .map_err(EngineHostError::ProducerHandoff)?
    else {
        return Ok(false);
    };
    let (execution, deadline, materialized) = submission.into_parts();
    match permit.submit(driver, execution, deadline, materialized, now) {
        Ok(()) => {
            data.apply_produce_driver_input(now, ProducerInput::DriverAccepted { execution })
                .map_err(EngineHostError::Producer)?;
        }
        Err(rejection) => {
            debug_assert_eq!(
                rejection.delivery(),
                kafka_client_core::DeliveryStatus::NotSent
            );
            drop(rejection);
            data.apply_produce_driver_input(now, ProducerInput::DriverRejected { execution })
                .map_err(EngineHostError::Producer)?;
        }
    }
    Ok(true)
}

/// Applies at most `budget` terminal driver facts under the shard guard.
pub(super) fn apply_ready(
    calls: &mut TrackedProduceCalls,
    data: &mut ProducerShardData,
    now: Moment,
    budget: usize,
) -> Result<bool, EngineHostError> {
    let mut progress = false;
    for _attempt in 0..budget {
        let Some(settled) = calls
            .poll_next_ready()
            .map_err(EngineHostError::ProduceCompletion)?
        else {
            break;
        };
        data.apply_produce_driver_input(now, settled.input())
            .map_err(EngineHostError::Producer)?;
        calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}
