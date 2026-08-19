//! Fair host turns for leader-routed or exact-broker Admin `DescribeProducers`.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{AdminDescribeProducersShardLockError, AdminDescribeProducersTurn},
    driver::DescribeProducersCall,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct AdminDescribeProducersProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<AdminDescribeProducersProgress, EngineHostError> {
    let mut host = match resources.describe_producers.try_host() {
        Ok(host) => host,
        Err(AdminDescribeProducersShardLockError::Contended) => {
            return Ok(AdminDescribeProducersProgress::contended());
        }
        Err(AdminDescribeProducersShardLockError::Poisoned) => {
            return Err(EngineHostError::AdminDescribeProducersLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_producers.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::AdminDescribeProducers)?;
    let driver_progress = match turn {
        AdminDescribeProducersTurn::Idle => false,
        AdminDescribeProducersTurn::Progress => true,
        AdminDescribeProducersTurn::Submit(submission) => {
            let (operation_id, deadline, target, broker_id) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeProducersCall::submit(driver, &target, broker_id, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::AdminDescribeProducers)?,
                Err(_rejection) => {
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::AdminDescribeProducers)?;
                }
            }
            true
        }
    };
    Ok(AdminDescribeProducersProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl AdminDescribeProducersProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}
