//! Bounded host turns for concrete any-broker `DescribeCluster` work.

use kafka_client_core::{Deadline, DescribeClusterInput, Moment};

use crate::admin::{DescribeClusterShardLockError, DescribeClusterTurn};

use super::super::{EngineHostError, EngineHostResources};

const DESCRIBE_CLUSTER_COMPLETION_BUDGET: usize = 16;

pub(super) struct DescribeClusterProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeClusterProgress, EngineHostError> {
    let Some(permit) = resources.describe_cluster_calls.try_reserve() else {
        return snapshot(&resources.describe_cluster);
    };
    let mut host = match resources.describe_cluster.try_host() {
        Ok(host) => host,
        Err(DescribeClusterShardLockError::Contended) => {
            return Ok(DescribeClusterProgress::contended());
        }
        Err(DescribeClusterShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeClusterLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_cluster.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeCluster)?;
    let driver_progress = match turn {
        DescribeClusterTurn::Idle => false,
        DescribeClusterTurn::Progress => true,
        DescribeClusterTurn::Submit(submission) => {
            let (
                operation_id,
                deadline,
                retained_bytes,
                include_fenced_brokers,
                include_authorized_operations,
            ) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match permit.submit(
                driver,
                operation_id,
                deadline,
                retained_bytes,
                include_fenced_brokers,
                include_authorized_operations,
            ) {
                Ok(()) => host
                    .apply(operation_id, DescribeClusterInput::DriverAccepted)
                    .map_err(EngineHostError::DescribeCluster)?,
                Err(rejection) => host
                    .apply(operation_id, rejection.into_core_input())
                    .map_err(EngineHostError::DescribeCluster)?,
            }
            true
        }
    };
    Ok(DescribeClusterProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

pub(super) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let mut host = match resources.describe_cluster.try_host() {
        Ok(host) => host,
        Err(DescribeClusterShardLockError::Contended) => return Ok(false),
        Err(DescribeClusterShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeClusterLockPoisoned);
        }
    };
    let mut progress = false;
    for _attempt in 0..DESCRIBE_CLUSTER_COMPLETION_BUDGET {
        let Some(settled) = resources
            .describe_cluster_calls
            .poll_next_ready()
            .map_err(EngineHostError::DescribeClusterCompletion)?
        else {
            break;
        };
        let operation_id = settled.operation_id();
        let input = settled
            .take_input()
            .ok_or(EngineHostError::DescribeCluster(
                crate::admin::DescribeClusterHostError::MissingTerminal,
            ))?;
        host.apply(operation_id, input)
            .map_err(EngineHostError::DescribeCluster)?;
        resources.describe_cluster_calls.discard_settled();
        progress = true;
    }
    Ok(progress)
}

fn snapshot(
    owner: &crate::admin::DescribeClusterShardOwner,
) -> Result<DescribeClusterProgress, EngineHostError> {
    let host = match owner.try_host() {
        Ok(host) => host,
        Err(DescribeClusterShardLockError::Contended) => {
            return Ok(DescribeClusterProgress::contended());
        }
        Err(DescribeClusterShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeClusterLockPoisoned);
        }
    };
    Ok(DescribeClusterProgress {
        unsettled: host.unsettled(),
        driver_progress: false,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeClusterProgress {
    pub(super) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
