//! Fair host turns for controller-routed partition-reassignment listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{ListPartitionReassignmentsShardLockError, ListPartitionReassignmentsTurn},
    driver::ListPartitionReassignmentsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListPartitionReassignmentsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListPartitionReassignmentsProgress, EngineHostError> {
    let mut host = match resources.list_partition_reassignments.try_host() {
        Ok(host) => host,
        Err(ListPartitionReassignmentsShardLockError::Contended) => {
            return Ok(ListPartitionReassignmentsProgress::contended());
        }
        Err(ListPartitionReassignmentsShardLockError::Poisoned) => {
            return Err(EngineHostError::ListPartitionReassignmentsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .list_partition_reassignments
            .close_locked(&mut host);
    }
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turn = host
        .turn_with_driver(now, driver)
        .map_err(EngineHostError::ListPartitionReassignments)?;
    let driver_progress = match turn {
        ListPartitionReassignmentsTurn::Idle => false,
        ListPartitionReassignmentsTurn::Progress => true,
        ListPartitionReassignmentsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, result_limit) = submission.into_parts();
            match ListPartitionReassignmentsCall::submit(driver, plan, result_limit, now, deadline)
            {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListPartitionReassignments)?,
                Err(rejection) => {
                    let (plan, result_limit) = rejection.into_correlation();
                    host.reject_handoff(operation_id, plan, result_limit)
                        .map_err(EngineHostError::ListPartitionReassignments)?;
                }
            }
            true
        }
    };
    Ok(ListPartitionReassignmentsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ListPartitionReassignmentsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
