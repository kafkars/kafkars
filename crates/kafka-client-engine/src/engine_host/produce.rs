//! Atomic join between prepared producer ownership and tracked driver calls.

mod partitioning;
mod routing;
#[cfg(test)]
mod routing_test;

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
pub(super) use routing::{
    ProducerRoutingCall, apply_ready as apply_routing_ready,
    discard_after_driver_shutdown as discard_routing_after_driver_shutdown,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ProduceAdmissionOutcome {
    progressed: bool,
    prepared_batches: usize,
}

impl ProduceAdmissionOutcome {
    pub(super) const fn idle() -> Self {
        Self {
            progressed: false,
            prepared_batches: 0,
        }
    }

    pub(super) const fn progressed(prepared_batches: usize) -> Self {
        Self {
            progressed: true,
            prepared_batches,
        }
    }

    pub(super) const fn did_progress(self) -> bool {
        self.progressed
    }

    pub(super) const fn prepared_batches(self) -> usize {
        self.prepared_batches
    }
}

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

pub(super) fn admit_one(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    routing: &mut Option<ProducerRoutingCall>,
    data: &mut ProducerShardData,
    now: Moment,
    prepared_batch_budget: usize,
) -> Result<ProduceAdmissionOutcome, EngineHostError> {
    if prepared_batch_budget == 0 {
        return Ok(ProduceAdmissionOutcome::idle());
    }
    if let Some(progress) =
        routing::admit(driver, calls, routing, data, now, prepared_batch_budget)?
    {
        return Ok(progress);
    }
    let Some(key) = data.next_produce_route_key() else {
        return Ok(ProduceAdmissionOutcome::idle());
    };
    *routing = Some(ProducerRoutingCall::new(key));
    routing::admit(driver, calls, routing, data, now, prepared_batch_budget)
        .map(|progress| progress.unwrap_or_else(|| ProduceAdmissionOutcome::progressed(0)))
}

pub(super) fn reject_execution(
    data: &mut ProducerShardData,
    execution: kafka_client_core::BatchExecutionId,
    now: Moment,
    failure: kafka_client_core::ProducerAttemptFailureKind,
) -> Result<(), EngineHostError> {
    data.apply_produce_driver_input(
        now,
        ProducerInput::DriverRejected {
            execution,
            now,
            failure,
        },
    )
    .map_err(EngineHostError::Producer)
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
            ProduceRouteRefreshPoll::Ready | ProduceRouteRefreshPoll::Failed => {}
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
