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
        .producer_retry_identity_call
        .as_ref()
        .map(produce::ProducerRetryIdentityCall::deadline)
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
    let produce_admissions = admit_ready(
        driver,
        &mut resources.produce_calls,
        &mut resources.producer_retry_identity_call,
        &mut data,
        now,
        retained_partitioning_deadline,
    )?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: data
            .unsettled_completions()
            .saturating_add(usize::from(resources.producer_partitioning_call.is_some()))
            .saturating_add(usize::from(
                resources.producer_retry_identity_call.is_some(),
            )),
        admissions: produce_admissions,
        driver_progress: identity_progress || partitioning_progress || produce_admissions != 0,
    })
}

fn admit_ready(
    driver: &crate::driver::DriverOwner,
    calls: &mut crate::driver::TrackedProduceCalls,
    retry_identity: &mut Option<produce::ProducerRetryIdentityCall>,
    data: &mut crate::producer::ingress::ProducerShardData,
    now: Moment,
    retained_partitioning_deadline: Option<crate::clock::OperationDeadline>,
) -> Result<usize, EngineHostError> {
    let mut admissions = 0_usize;
    for _attempt in 0..PRODUCE_ADMISSION_BUDGET {
        if !admit_after_partitioning(
            driver,
            calls,
            retry_identity,
            data,
            now,
            retained_partitioning_deadline,
        )? {
            break;
        }
        admissions = admissions.saturating_add(1);
    }
    Ok(admissions)
}

pub(super) fn admit_after_partitioning(
    driver: &crate::driver::DriverOwner,
    calls: &mut crate::driver::TrackedProduceCalls,
    retry_identity: &mut Option<produce::ProducerRetryIdentityCall>,
    data: &mut crate::producer::ingress::ProducerShardData,
    now: Moment,
    retained_partitioning_deadline: Option<crate::clock::OperationDeadline>,
) -> Result<bool, EngineHostError> {
    if let Some(ready_deadline) = data.next_produce_submission_deadline() {
        if retained_partitioning_deadline == Some(ready_deadline)
            || data.has_pending_produce_submission_at(ready_deadline)
        {
            return Ok(false);
        }
    }
    produce::admit_one(driver, calls, retry_identity, data, now)
}

pub(super) fn apply_completions(
    driver: &crate::driver::DriverOwner,
    producer: &ProducerShardOwner,
    identity_calls: &mut crate::driver::TrackedProducerIdentityCalls,
    partitioning_call: &mut Option<produce::ProducerPartitioningCall>,
    retry_identity_call: &mut Option<produce::ProducerRetryIdentityCall>,
    calls: &mut crate::driver::TrackedProduceCalls,
    now: Moment,
) -> Result<bool, EngineHostError> {
    let mut data = match producer.try_data() {
        Ok(data) => data,
        Err(ProducerShardLockError::Contended) => return Ok(false),
        Err(ProducerShardLockError::Poisoned) => {
            return Err(EngineHostError::ProducerLockPoisoned);
        }
    };
    let identity = produce::apply_identity_ready(identity_calls, &mut data, now)?;
    let partitioning = produce::apply_partitioning_ready(partitioning_call, &mut data)?;
    let retry_identity = produce::apply_retry_identity_ready(retry_identity_call, &mut data, now)?;
    let produce = produce::apply_ready(driver, calls, &mut data, now, PRODUCE_COMPLETION_BUDGET)?;
    Ok(identity || partitioning || retry_identity || produce)
}

impl ProducerProgress {
    const fn contended() -> Self {
        Self {
            outcome: None,
            unsettled: usize::MAX,
            admissions: 0,
            driver_progress: false,
        }
    }
}
