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
    let outcome = data
        .turn(now, resources.budget)
        .map_err(EngineHostError::Producer)?;
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let driver_progress = produce::admit_one(driver, &mut resources.produce_calls, &mut data, now)?;
    Ok(ProducerProgress {
        outcome: Some(outcome),
        unsettled: data.unsettled_completions(),
        driver_progress,
    })
}

pub(super) fn apply_completions(
    producer: &ProducerShardOwner,
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
    produce::apply_ready(calls, &mut data, now, PRODUCE_COMPLETION_BUDGET)
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
