//! Bounded producer-shard turns joined to tracked driver admission and settlement.

use kafka_client_core::Moment;

use crate::producer::{
    host_turn::ProducerTurnOutcome,
    ingress::{ProducerShardLockError, ProducerShardOwner},
};

use super::{EngineHostError, EngineHostResources, produce};

const PRODUCE_COMPLETION_BUDGET: usize = 64;
const PRODUCE_ADMISSION_BUDGET: usize = 64;

pub(super) struct ProducerProgress {
    pub(super) outcome: Option<ProducerTurnOutcome>,
    pub(super) unsettled: usize,
    pub(super) admissions: usize,
    pub(super) driver_progress: bool,
}

pub(super) struct ProducerCompletionProgress {
    pub(super) progressed: bool,
    pub(super) prepared_batches: usize,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ProducerProgress, EngineHostError> {
    let mut data = match resources.producer.try_data() {
        Ok(data) => data,
        Err(ProducerShardLockError::Contended) => return Ok(ProducerProgress::contended()),
        Err(ProducerShardLockError::Poisoned) => {
            return Err(EngineHostError::ProducerLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.producer.close_locked_admission(&mut data);
    }
    resources.control.record_producer_turn();
    let mut outcome = data
        .turn(now, resources.budget)
        .map_err(EngineHostError::Producer)?;
    if let Some(deadline) = resources.produce_calls.next_refresh_deadline() {
        outcome.next_deadline = Some(
            outcome
                .next_deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
    }
    if let Some(deadline) = resources
        .producer_routing_call
        .as_ref()
        .and_then(produce::ProducerRoutingCall::deadline)
        .map(crate::clock::OperationDeadline::core)
    {
        outcome.next_deadline = Some(
            outcome
                .next_deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
    }
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let identity_progress = produce::admit_identity(
        driver,
        &mut resources.producer_identity_calls,
        &mut data,
        now,
    )?;
    let partitioning_progress =
        produce::admit_partitioning(driver, &mut resources.producer_partitioning_call, &mut data)?;
    let retained_partitioning_deadline = resources
        .producer_partitioning_call
        .as_ref()
        .map(produce::ProducerPartitioningCall::deadline);
    let produce_admission = admit_ready(
        driver,
        &mut resources.produce_calls,
        &mut resources.producer_routing_call,
        &mut data,
        now,
        retained_partitioning_deadline,
    )?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: data
            .unsettled_completions()
            .saturating_add(usize::from(resources.producer_partitioning_call.is_some()))
            .saturating_add(usize::from(resources.producer_routing_call.is_some())),
        admissions: produce_admission.prepared_batches(),
        driver_progress: identity_progress
            || partitioning_progress
            || produce_admission.did_progress(),
    })
}

fn admit_ready(
    driver: &crate::driver::DriverOwner,
    calls: &mut crate::driver::TrackedProduceCalls,
    routing: &mut Option<produce::ProducerRoutingCall>,
    data: &mut crate::producer::ingress::ProducerShardData,
    now: Moment,
    retained_partitioning_deadline: Option<crate::clock::OperationDeadline>,
) -> Result<produce::ProduceAdmissionOutcome, EngineHostError> {
    let mut admissions = 0_usize;
    let mut progressed = false;
    for _attempt in 0..PRODUCE_ADMISSION_BUDGET {
        let remaining = PRODUCE_ADMISSION_BUDGET.saturating_sub(admissions);
        if remaining == 0 {
            break;
        }
        let outcome = admit_after_partitioning(
            driver,
            calls,
            routing,
            data,
            now,
            retained_partitioning_deadline,
            remaining,
        )?;
        if !outcome.did_progress() {
            break;
        }
        progressed = true;
        debug_assert!(outcome.prepared_batches() <= remaining);
        admissions = admissions.saturating_add(outcome.prepared_batches());
    }
    Ok(if progressed {
        produce::ProduceAdmissionOutcome::progressed(admissions)
    } else {
        produce::ProduceAdmissionOutcome::idle()
    })
}

pub(super) fn admit_after_partitioning(
    driver: &crate::driver::DriverOwner,
    calls: &mut crate::driver::TrackedProduceCalls,
    routing: &mut Option<produce::ProducerRoutingCall>,
    data: &mut crate::producer::ingress::ProducerShardData,
    now: Moment,
    retained_partitioning_deadline: Option<crate::clock::OperationDeadline>,
    prepared_batch_budget: usize,
) -> Result<produce::ProduceAdmissionOutcome, EngineHostError> {
    if let Some(ready_deadline) = data.next_produce_submission_deadline() {
        if retained_partitioning_deadline == Some(ready_deadline)
            || data.has_pending_produce_submission_at(ready_deadline)
        {
            return Ok(produce::ProduceAdmissionOutcome::idle());
        }
    }
    produce::admit_one(driver, calls, routing, data, now, prepared_batch_budget)
}

#[expect(
    clippy::too_many_arguments,
    reason = "separate linear owners keep completion mutation explicit and borrow-scoped"
)]
pub(super) fn apply_completions(
    driver: &crate::driver::DriverOwner,
    producer: &ProducerShardOwner,
    identity_calls: &mut crate::driver::TrackedProducerIdentityCalls,
    partitioning_call: &mut Option<produce::ProducerPartitioningCall>,
    routing_call: &mut Option<produce::ProducerRoutingCall>,
    calls: &mut crate::driver::TrackedProduceCalls,
    now: Moment,
    prepared_batch_budget: usize,
) -> Result<ProducerCompletionProgress, EngineHostError> {
    let mut data = match producer.try_data() {
        Ok(data) => data,
        Err(ProducerShardLockError::Contended) => {
            return Ok(ProducerCompletionProgress {
                progressed: false,
                prepared_batches: 0,
            });
        }
        Err(ProducerShardLockError::Poisoned) => {
            return Err(EngineHostError::ProducerLockPoisoned);
        }
    };
    let identity = produce::apply_identity_ready(identity_calls, &mut data, now)?;
    let partitioning = produce::apply_partitioning_ready(partitioning_call, &mut data)?;
    let routing =
        produce::apply_routing_ready(routing_call, &mut data, now, prepared_batch_budget)?;
    let produce = produce::apply_ready(driver, calls, &mut data, now, PRODUCE_COMPLETION_BUDGET)?;
    Ok(ProducerCompletionProgress {
        progressed: identity || partitioning || routing.did_progress() || produce,
        prepared_batches: routing.prepared_batches(),
    })
}

impl ProducerProgress {
    pub(super) const fn remaining_admission_budget(&self) -> usize {
        PRODUCE_ADMISSION_BUDGET.saturating_sub(self.admissions)
    }

    const fn contended() -> Self {
        Self {
            outcome: None,
            unsettled: usize::MAX,
            admissions: 0,
            driver_progress: false,
        }
    }
}
