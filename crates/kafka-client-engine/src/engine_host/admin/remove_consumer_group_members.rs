//! Fair host turns for coordinator-routed static-member removal.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{RemoveConsumerGroupMembersShardLockError, RemoveConsumerGroupMembersTurn},
    driver::RemoveConsumerGroupMembersCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct RemoveConsumerGroupMembersProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<RemoveConsumerGroupMembersProgress, EngineHostError> {
    let mut host = match resources.remove_consumer_group_members.try_host() {
        Ok(host) => host,
        Err(RemoveConsumerGroupMembersShardLockError::Contended) => {
            return Ok(RemoveConsumerGroupMembersProgress::contended());
        }
        Err(RemoveConsumerGroupMembersShardLockError::Poisoned) => {
            return Err(EngineHostError::RemoveConsumerGroupMembersLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .remove_consumer_group_members
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::RemoveConsumerGroupMembers)?;
    let driver_progress = match turn {
        RemoveConsumerGroupMembersTurn::Idle => false,
        RemoveConsumerGroupMembersTurn::Progress => true,
        RemoveConsumerGroupMembersTurn::Submit(submission) => {
            let (operation_id, deadline, plan, scratch_limit, result_limit) =
                submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match RemoveConsumerGroupMembersCall::submit(
                driver,
                plan,
                scratch_limit,
                result_limit,
                deadline,
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::RemoveConsumerGroupMembers)?,
                Err(rejection) => {
                    let (plan, scratch_limit, result_limit) = rejection.into_correlation();
                    host.reject_handoff(operation_id, plan, scratch_limit, result_limit)
                        .map_err(EngineHostError::RemoveConsumerGroupMembers)?;
                }
            }
            true
        }
    };
    Ok(RemoveConsumerGroupMembersProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl RemoveConsumerGroupMembersProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
