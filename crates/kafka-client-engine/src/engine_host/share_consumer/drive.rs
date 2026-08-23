//! One bounded private share-consumer registry turn on the embedded host.

use kafka_client_core::{Deadline, Moment};
use std::time::Duration;

use crate::{
    consumer::{
        ShareConsumerRegistry, ShareConsumerShardLockError, ShareConsumerShardOwner,
        ShareMembershipTurn,
    },
    driver::DriverOwner,
};

use super::super::{EngineHostError, EngineHostResources};

const SHARE_CONTROL_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

pub(in crate::engine_host) struct ShareConsumerProgress {
    pub(in crate::engine_host) unsettled: usize,
    pub(in crate::engine_host) progressed: bool,
    pub(in crate::engine_host) blocked_work: bool,
    pub(in crate::engine_host) next_deadline: Option<Deadline>,
}

pub(in crate::engine_host) fn drive(
    resources: &EngineHostResources,
    stage_now: Moment,
) -> Result<ShareConsumerProgress, EngineHostError> {
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    drive_shard(
        &resources.share_consumers,
        &resources.clock,
        driver,
        resources.control.shutdown_requested(),
        stage_now,
    )
}

pub(super) fn drive_shard(
    shard: &ShareConsumerShardOwner,
    clock: &crate::clock::MonotonicClock,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<ShareConsumerProgress, EngineHostError> {
    if shutdown {
        shard.close_admission();
    }
    let mut registry = match shard.try_registry_for_host_turn() {
        Ok(registry) => registry,
        Err(ShareConsumerShardLockError::Contended) => {
            return Ok(ShareConsumerProgress::contended());
        }
        Err(ShareConsumerShardLockError::Poisoned) => {
            return Err(EngineHostError::ShareConsumerLockPoisoned);
        }
    };
    if shutdown && registry.has_unclosed_entries() {
        let capture = clock
            .capture_deadline_after(SHARE_CONTROL_CLOSE_TIMEOUT)
            .map_err(EngineHostError::Clock)?;
        registry.request_control_close(capture);
    }
    drive_registry(&mut registry, clock, driver, stage_now)
}

pub(super) fn drive_registry(
    registry: &mut ShareConsumerRegistry,
    clock: &crate::clock::MonotonicClock,
    driver: &DriverOwner,
    stage_now: Moment,
) -> Result<ShareConsumerProgress, EngineHostError> {
    let turn = registry
        .turn_membership(stage_now, clock, driver)
        .map_err(EngineHostError::ShareConsumer)?;
    Ok(ShareConsumerProgress {
        unsettled: registry.unsettled(),
        progressed: turn == ShareMembershipTurn::Progress,
        blocked_work: turn == ShareMembershipTurn::Blocked,
        next_deadline: registry.next_deadline(),
    })
}

impl ShareConsumerProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            progressed: false,
            blocked_work: true,
            next_deadline: None,
        }
    }
}
