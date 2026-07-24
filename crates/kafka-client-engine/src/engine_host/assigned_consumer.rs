//! One concrete assigned-consumer turn joined to the embedded driver owner.

use kafka_client_core::{Deadline, Moment};

use crate::{
    consumer::{
        AssignedConsumerOwner, AssignedConsumerShardLockError, AssignedConsumerShardOwner,
        AssignedConsumerShutdownStart,
    },
    driver::DriverOwner,
};

use super::{EngineHostError, EngineHostResources};

pub(super) struct AssignedConsumerProgress {
    pub(super) unsettled: usize,
    pub(super) progressed: bool,
    pub(super) blocked_work: bool,
    pub(super) next_deadline: Option<Deadline>,
    pub(super) close_completed: bool,
}

pub(super) fn drive(
    resources: &EngineHostResources,
    stage_now: Moment,
) -> Result<AssignedConsumerProgress, EngineHostError> {
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let shutdown = resources.control.shutdown_requested();
    drive_shard(&resources.assigned_consumer, driver, shutdown, stage_now)
}

pub(super) fn drive_shard(
    shard: &AssignedConsumerShardOwner,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<AssignedConsumerProgress, EngineHostError> {
    match shard.try_with_owner(|owner| drive_locked(shard, owner, driver, shutdown, stage_now)) {
        Ok(result) => result,
        Err(AssignedConsumerShardLockError::Contended) => Ok(AssignedConsumerProgress::contended()),
        Err(AssignedConsumerShardLockError::Poisoned) => {
            Err(EngineHostError::AssignedConsumerLockPoisoned)
        }
        Err(AssignedConsumerShardLockError::OwnerMissing) => {
            Err(EngineHostError::AssignedConsumerOwnerMissing)
        }
    }
}

fn drive_locked(
    shard: &AssignedConsumerShardOwner,
    owner: &mut AssignedConsumerOwner,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<AssignedConsumerProgress, EngineHostError> {
    let close = if shutdown {
        shard
            .begin_shutdown(owner)
            .map_err(EngineHostError::AssignedConsumer)?
    } else {
        AssignedConsumerShutdownStart::AlreadyStarted
    };
    let turn = owner.turn(driver);
    if let Some(fault) = owner.fault_kind() {
        return Err(EngineHostError::AssignedConsumerFault(fault));
    }
    let progressed = close == AssignedConsumerShutdownStart::Started || turn.progressed();
    let unsettled = owner.unsettled();
    let next_deadline = owner.next_deadline();
    let deadline_due = next_deadline.is_some_and(|deadline| deadline.tick() <= stage_now.tick());
    Ok(AssignedConsumerProgress {
        unsettled,
        progressed,
        blocked_work: close == AssignedConsumerShutdownStart::Pending
            || (!progressed && (unsettled != 0 || deadline_due)),
        next_deadline,
        close_completed: owner.close_completed(),
    })
}

impl AssignedConsumerProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            progressed: false,
            blocked_work: true,
            next_deadline: None,
            close_completed: false,
        }
    }
}
