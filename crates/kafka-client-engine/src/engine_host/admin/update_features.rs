//! Fair host turns for destructive controller-routed Admin `UpdateFeatures`.

use kafka_client_core::{Deadline, Moment, UpdateFeatureIntent, UpdateFeaturesPlan};

use crate::{
    admin::update_features::{
        UpdateFeaturesShardLockError, UpdateFeaturesShardWake, UpdateFeaturesShardWakeError,
        UpdateFeaturesTurn,
    },
    driver::{ReactorWake, UpdateFeaturesCall},
    protocol::admin::update_features::{
        PreparedUpdateFeaturesRequest, UpdateFeatureMode, UpdateFeatureRef,
        UpdateFeaturesRequestPlan, update_features_request,
    },
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
    let turn = host.turn(now).map_err(EngineHostError::UpdateFeatures)?;
    let driver_progress = match turn {
        UpdateFeaturesTurn::Idle => false,
        UpdateFeaturesTurn::Progress => true,
        UpdateFeaturesTurn::Submit(submission) => {
            let (operation_id, deadline, plan, result_limit) = submission.into_parts();
            let request = remaining_timeout_ms(now, deadline.core())
                .and_then(|timeout_ms| materialize_request(&plan, timeout_ms, result_limit));
            let Some((request, minimum_version)) = request else {
                host.reject_handoff(operation_id)
                    .map_err(EngineHostError::UpdateFeatures)?;
                return Ok(UpdateFeaturesProgress {
                    unsettled: host.unsettled(),
                    driver_progress: true,
                    next_deadline: host.next_deadline(),
                });
            };
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match UpdateFeaturesCall::submit(driver, request, minimum_version, deadline.transport())
            {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::UpdateFeatures)?,
                Err(rejection) => {
                    drop(rejection);
                    host.reject_handoff(operation_id)
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

fn remaining_timeout_ms(now: Moment, deadline: Deadline) -> Option<i32> {
    let remaining = deadline
        .tick()
        .checked_sub(now.tick())
        .filter(|remaining| *remaining > 0)?;
    let milliseconds = remaining.saturating_add(999_999) / 1_000_000;
    Some(i32::try_from(milliseconds).unwrap_or(i32::MAX))
}

fn materialize_request(
    plan: &UpdateFeaturesPlan,
    timeout_ms: i32,
    result_limit: usize,
) -> Option<(PreparedUpdateFeaturesRequest, i16)> {
    let mut updates = Vec::new();
    updates.try_reserve_exact(plan.updates().len()).ok()?;
    updates.extend(plan.updates().iter().map(|update| {
        UpdateFeatureRef::new(
            update.feature(),
            update.max_version_level(),
            match update.intent() {
                UpdateFeatureIntent::Upgrade => UpdateFeatureMode::Upgrade,
                UpdateFeatureIntent::SafeDowngrade => UpdateFeatureMode::SafeDowngrade,
                UpdateFeatureIntent::UnsafeDowngrade => UpdateFeatureMode::UnsafeDowngrade,
            },
        )
    }));
    update_features_request(
        UpdateFeaturesRequestPlan::new(&updates, plan.validate_only()),
        timeout_ms,
        result_limit,
    )
    .ok()
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
