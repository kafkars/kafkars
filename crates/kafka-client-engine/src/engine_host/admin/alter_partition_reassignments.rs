//! Fair host turns for controller-routed partition-reassignment alteration.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AlterPartitionReassignmentsShardLockError, AlterPartitionReassignmentsTurn},
    driver::AlterPartitionReassignmentsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AlterPartitionReassignmentsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AlterPartitionReassignmentsProgress, EngineHostError> {
    let mut host = match resources.alter_partition_reassignments.try_host() {
        Ok(host) => host,
        Err(AlterPartitionReassignmentsShardLockError::Contended) => {
            return Ok(AlterPartitionReassignmentsProgress::contended());
        }
        Err(AlterPartitionReassignmentsShardLockError::Poisoned) => {
            return Err(EngineHostError::AlterPartitionReassignmentsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .alter_partition_reassignments
            .close_locked(&mut host);
    }
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turn = host
        .turn_with_driver(now, driver)
        .map_err(EngineHostError::AlterPartitionReassignments)?;
    let driver_progress = match turn {
        AlterPartitionReassignmentsTurn::Idle => false,
        AlterPartitionReassignmentsTurn::Progress => true,
        AlterPartitionReassignmentsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, scratch_limit, result_limit) =
                submission.into_parts();
            match AlterPartitionReassignmentsCall::submit(
                driver,
                plan,
                scratch_limit,
                result_limit,
                deadline,
                now,
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AlterPartitionReassignments)?,
                Err(rejection) => {
                    let (plan, scratch_limit, result_limit) = rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, scratch_limit, result_limit)
                        .map_err(EngineHostError::AlterPartitionReassignments)?;
                }
            }
            true
        }
    };
    Ok(AlterPartitionReassignmentsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AlterPartitionReassignmentsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
