//! Fair host turns for sequential coordinator-routed group descriptions.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{DescribeConsumerGroupsShardLockError, DescribeConsumerGroupsTurn},
    driver::DescribeConsumerGroupsCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeConsumerGroupsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeConsumerGroupsProgress, EngineHostError> {
    let mut host = match resources.describe_consumer_groups.try_host() {
        Ok(host) => host,
        Err(DescribeConsumerGroupsShardLockError::Contended) => {
            return Ok(DescribeConsumerGroupsProgress::contended());
        }
        Err(DescribeConsumerGroupsShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeConsumerGroupsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_consumer_groups.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::DescribeConsumerGroups)?;
    let driver_progress = match turn {
        DescribeConsumerGroupsTurn::Idle => false,
        DescribeConsumerGroupsTurn::Progress => true,
        DescribeConsumerGroupsTurn::Submit(submission) => {
            let (
                operation_id,
                deadline,
                group_id,
                include_authorized_operations,
                call_kind,
                request_scratch_limit,
                result_limit,
            ) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeConsumerGroupsCall::submit(
                driver,
                call_kind,
                group_id,
                include_authorized_operations,
                request_scratch_limit,
                result_limit,
                deadline,
            ) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeConsumerGroups)?,
                Err(rejection) => {
                    let (
                        group_id,
                        include_authorized_operations,
                        call_kind,
                        request_scratch_limit,
                        result_limit,
                    ) = rejection.into_evidence();
                    host.reject_handoff(
                        operation_id,
                        group_id,
                        include_authorized_operations,
                        call_kind,
                        request_scratch_limit,
                        result_limit,
                    )
                    .map_err(EngineHostError::DescribeConsumerGroups)?;
                }
            }
            true
        }
    };
    Ok(DescribeConsumerGroupsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeConsumerGroupsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
