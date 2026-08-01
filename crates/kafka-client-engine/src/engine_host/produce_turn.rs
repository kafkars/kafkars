//! Bounded producer-shard turns joined to tracked driver admission and settlement.

use kafka_client_core::Moment;

use crate::producer::{
    host_turn::ProducerTurnOutcome,
    ingress::{ProducerShardLockError, ProducerShardOwner},
};

use super::{EngineHostError, EngineHostResources, produce};

const PRODUCE_COMPLETION_BUDGET: usize = 64;

pub(super) struct ProducerProgress {
    pub(super) outcome: Option<ProducerTurnOutcome>,
    pub(super) unsettled: usize,
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
    let produce_progress =
        produce::admit_one(driver, &mut resources.produce_calls, &mut data, now)?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: data
            .unsettled_completions()
            .saturating_add(usize::from(resources.producer_partitioning_call.is_some())),
        driver_progress: identity_progress || partitioning_progress || produce_progress,
    })
}

pub(super) fn apply_completions(
    driver: &crate::driver::DriverOwner,
    producer: &ProducerShardOwner,
    identity_calls: &mut crate::driver::TrackedProducerIdentityCalls,
    partitioning_call: &mut Option<produce::ProducerPartitioningCall>,
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
    let produce = produce::apply_ready(driver, calls, &mut data, now, PRODUCE_COMPLETION_BUDGET)?;
    Ok(identity || partitioning || produce)
}

impl ProducerProgress {
    const fn contended() -> Self {
        Self {
            outcome: None,
            unsettled: usize::MAX,
            driver_progress: false,
        }
    }
}
