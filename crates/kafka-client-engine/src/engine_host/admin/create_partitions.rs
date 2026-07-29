//! Bounded host turns for controller-routed `CreatePartitions` work.

use kafka_client_core::{CreatePartitionsInput, Deadline, Moment};

use crate::{
    admin::{CreatePartitionsShardLockError, CreatePartitionsTurn},
    driver::CreatePartitionsControllerRefreshPoll,
};

use super::super::{EngineHostError, EngineHostResources};

const CREATE_PARTITIONS_COMPLETION_BUDGET: usize = 16;

pub(super) struct CreatePartitionsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<CreatePartitionsProgress, EngineHostError> {
    let Some(permit) = resources.create_partitions_calls.try_reserve() else {
        return snapshot(&resources.create_partitions);
    };
    let mut host = match resources.create_partitions.try_host() {
        Ok(host) => host,
        Err(CreatePartitionsShardLockError::Contended) => {
            return Ok(CreatePartitionsProgress::contended());
        }
        Err(CreatePartitionsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreatePartitionsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.create_partitions.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::CreatePartitions)?;
    let driver_progress = match turn {
        CreatePartitionsTurn::Idle => false,
        CreatePartitionsTurn::Progress => true,
        CreatePartitionsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_bytes) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(driver, operation_id, deadline, plan, retained_bytes, now) {
                Ok(()) => host
                    .apply(operation_id, CreatePartitionsInput::DriverAccepted)
                    .map_err(EngineHostError::CreatePartitions)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.core_input())
                    .map_err(EngineHostError::CreatePartitions)?,
            }
            true
        }
    };
    Ok(CreatePartitionsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.create_partitions.try_host() {
        Ok(host) => host,
        Err(CreatePartitionsShardLockError::Contended) => return Ok(false),
        Err(CreatePartitionsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreatePartitionsLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..CREATE_PARTITIONS_COMPLETION_BUDGET {
        let Some(settled) = resources
            .create_partitions_calls
            .poll_next_ready()
            .map_err(EngineHostError::CreatePartitionsCompletion)?
        else {
            break;
        };
        match settled.poll_controller_refresh(resources.driver.as_ref()) {
            CreatePartitionsControllerRefreshPoll::Ready => {}
            CreatePartitionsControllerRefreshPoll::Pending => break,
            CreatePartitionsControllerRefreshPoll::DriverMissing => {
                return Err(EngineHostError::DriverOwnerMissing);
            }
        }
        let operation_id = settled.operation_id();
        let input = settled
            .take_input()
            .ok_or(EngineHostError::CreatePartitions(
                crate::admin::CreatePartitionsHostError::MissingTerminal,
            ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::CreatePartitions)?;
        resources.create_partitions_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::CreatePartitionsShardOwner,
) -> Result<CreatePartitionsProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(CreatePartitionsShardLockError::Contended) => {
            return Ok(CreatePartitionsProgress::contended());
        }
        Err(CreatePartitionsShardLockError::Poisoned) => {
            return Err(EngineHostError::CreatePartitionsLockPoisoned);
        }
    };
    Ok(CreatePartitionsProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl CreatePartitionsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
