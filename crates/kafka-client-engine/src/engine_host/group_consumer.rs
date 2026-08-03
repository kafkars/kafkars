//! One bounded private classic-group registry turn on the embedded host.

use kafka_client_core::{Deadline, Moment};

use crate::{
    consumer::{GroupConsumerRegistry, GroupConsumerShardLockError, GroupConsumerShardOwner},
    driver::DriverOwner,
};

use super::{EngineHostError, EngineHostResources};

pub(super) struct GroupConsumerProgress {
    pub(super) unsettled: usize,
    pub(super) progressed: bool,
    pub(super) blocked_work: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &EngineHostResources,
    stage_now: Moment,
) -> Result<GroupConsumerProgress, EngineHostError> {
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    drive_shard(
        &resources.group_consumers,
        &resources.clock,
        driver,
        resources.control.shutdown_requested(),
        stage_now,
    )
}

pub(super) fn drive_shard(
    shard: &GroupConsumerShardOwner,
    clock: &crate::clock::MonotonicClock,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<GroupConsumerProgress, EngineHostError> {
    if shutdown {
        shard.close_admission();
    }
    let mut registry = match shard.try_registry_for_host_turn() {
        Ok(registry) => registry,
        Err(GroupConsumerShardLockError::Contended) => {
            return Ok(GroupConsumerProgress::contended());
        }
        Err(GroupConsumerShardLockError::Poisoned) => {
            return Err(EngineHostError::GroupConsumerLockPoisoned);
        }
    };
    let progress = drive_registry(&mut registry, clock, driver, shutdown, stage_now)?;
    let notify_observation = progress.progressed;
    drop(registry);
    if notify_observation {
        shard.notify_recv_change();
    }
    Ok(progress)
}

pub(super) fn drive_registry(
    registry: &mut GroupConsumerRegistry,
    clock: &crate::clock::MonotonicClock,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<GroupConsumerProgress, EngineHostError> {
    if shutdown {
        registry.close_admission();
    }
    let turn = registry
        .turn(stage_now, clock, driver)
        .map_err(EngineHostError::GroupConsumer)?;
    Ok(GroupConsumerProgress {
        unsettled: registry.unsettled(),
        progressed: turn.progressed,
        blocked_work: turn.blocked_work,
        next_deadline: registry.next_deadline(),
    })
}

impl GroupConsumerProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            progressed: false,
            blocked_work: true,
            next_deadline: None,
        }
    }
}
