//! Fair host turns for AnyBroker Admin `DescribeTopicPartitions` work.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AdminDescribeTopicPartitionsShardLockError, AdminDescribeTopicPartitionsShardWake,
        AdminDescribeTopicPartitionsShardWakeError, AdminDescribeTopicPartitionsTurn,
    },
    driver::{DescribeTopicPartitionsCall, ReactorWake},
    protocol::admin::describe_topic_partitions::{
        DescribeTopicPartitionsRequestCursor, DescribeTopicPartitionsRequestPlan,
        describe_topic_partitions_request,
    },
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminDescribeTopicPartitionsProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminDescribeTopicPartitionsProgress, EngineHostError> {
    let mut host = match resources.describe_topic_partitions.try_host() {
        Ok(host) => host,
        Err(AdminDescribeTopicPartitionsShardLockError::Contended) => {
            return Ok(AdminDescribeTopicPartitionsProgress::contended());
        }
        Err(AdminDescribeTopicPartitionsShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminDescribeTopicPartitionsLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_topic_partitions.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AdminDescribeTopicPartitions)?;
    let driver_progress = match turn {
        AdminDescribeTopicPartitionsTurn::Idle => false,
        AdminDescribeTopicPartitionsTurn::Progress => true,
        AdminDescribeTopicPartitionsTurn::Submit(submission) => {
            let (operation_id, deadline, plan, retained_limit) = submission.into_parts();
            let cursor = plan.cursor().map(|cursor| {
                DescribeTopicPartitionsRequestCursor::new(
                    cursor.topic_name(),
                    cursor.partition_index(),
                )
            });
            let protocol_plan = DescribeTopicPartitionsRequestPlan::new(
                plan.topics(),
                plan.response_partition_limit(),
                cursor,
            );
            let Ok(request) = describe_topic_partitions_request(protocol_plan, retained_limit)
            else {
                host.reject_handoff(operation_id)
                    .map_err(EngineHostError::AdminDescribeTopicPartitions)?;
                return Ok(AdminDescribeTopicPartitionsProgress {
                    unsettled: host.unsettled(),
                    driver_progress: true,
                    next_deadline: host.next_deadline(),
                });
            };
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeTopicPartitionsCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AdminDescribeTopicPartitions)?,
                Err(rejection) => {
                    let _rejection = rejection;
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AdminDescribeTopicPartitions)?;
                }
            }
            true
        }
    };
    Ok(AdminDescribeTopicPartitionsProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminDescribeTopicPartitionsProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl AdminDescribeTopicPartitionsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminDescribeTopicPartitionsShardWakeError> {
        self.request()
            .map_err(|error| AdminDescribeTopicPartitionsShardWakeError::from_io(error.into_io()))
    }
}
