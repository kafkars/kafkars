//! Fair host turns for one explicit AnyBroker feature-metadata query.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::describe_features::{
        DescribeFeaturesShardLockError, DescribeFeaturesShardWake, DescribeFeaturesShardWakeError,
        DescribeFeaturesTurn,
    },
    driver::{DescribeFeaturesCall, ReactorWake},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct DescribeFeaturesProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<DescribeFeaturesProgress, EngineHostError> {
    let mut host = match resources.describe_features.try_host() {
        Ok(host) => host,
        Err(DescribeFeaturesShardLockError::Contended) => {
            return Ok(DescribeFeaturesProgress::contended());
        }
        Err(DescribeFeaturesShardLockError::Poisoned) => {
            return Err(EngineHostError::DescribeFeaturesLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.describe_features.close_locked(&mut host);
    }
    let turn = host.turn(now).map_err(EngineHostError::DescribeFeatures)?;
    let driver_progress = match turn {
        DescribeFeaturesTurn::Idle => false,
        DescribeFeaturesTurn::Progress => true,
        DescribeFeaturesTurn::Submit(submission) => {
            let (operation_id, deadline, _result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match DescribeFeaturesCall::submit(driver, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::DescribeFeatures)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::DescribeFeatures)?,
            }
            true
        }
    };
    Ok(DescribeFeaturesProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl DescribeFeaturesProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl DescribeFeaturesShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeFeaturesShardWakeError> {
        self.request()
            .map_err(|error| DescribeFeaturesShardWakeError::from_io(error.into_io()))
    }
}
