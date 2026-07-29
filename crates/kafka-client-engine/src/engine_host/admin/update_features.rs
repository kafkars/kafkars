//! Fair host turns for destructive controller-routed Admin `UpdateFeatures`.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::update_features::{
        UpdateFeaturesHostError, UpdateFeaturesShardLockError, UpdateFeaturesShardWake,
        UpdateFeaturesShardWakeError, UpdateFeaturesTurn,
    },
    driver::{ReactorWake, UpdateFeaturesCall},
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct UpdateFeaturesProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<UpdateFeaturesProgress, EngineHostError> {
    let mut host = match resources.update_features.try_host() {
        Ok(host) => host,
        Err(UpdateFeaturesShardLockError::Contended) => {
            return Ok(UpdateFeaturesProgress::contended());
        }
        Err(UpdateFeaturesShardLockError::Poisoned) => {
            return Err(EngineHostError::UpdateFeaturesLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.update_features.close_locked(&mut host);
    }
    let turn = match host.turn(now, resources.driver.as_ref()) {
        Ok(turn) => turn,
        Err(UpdateFeaturesHostError::DriverMissing) => {
            return Err(EngineHostError::DriverOwnerMissing);
        }
        Err(error) => return Err(EngineHostError::UpdateFeatures(error)),
    };
    let driver_progress = match turn {
        UpdateFeaturesTurn::Idle => false,
        UpdateFeaturesTurn::Progress => true,
        UpdateFeaturesTurn::Submit(submission) => {
            let (operation_id, deadline, plan, result_limit) = submission.into_parts();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match UpdateFeaturesCall::submit(driver, plan, result_limit, deadline, now) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::UpdateFeatures)?,
                Err(rejection) => {
                    let (plan, result_limit) = rejection.into_submission_evidence();
                    host.reject_handoff(operation_id, plan, result_limit)
                        .map_err(EngineHostError::UpdateFeatures)?;
                }
            }
            true
        }
    };
    Ok(UpdateFeaturesProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl UpdateFeaturesProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl UpdateFeaturesShardWake for ReactorWake {
    fn wake(&self) -> Result<(), UpdateFeaturesShardWakeError> {
        self.request()
            .map_err(|error| UpdateFeaturesShardWakeError::from_io(error.into_io()))
    }
}
