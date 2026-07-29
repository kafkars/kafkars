//! Fair host turns for AnyBroker Admin configuration-resource listing.

use kafka_client_core::{Deadline, ListConfigResourcesPlan, Moment};

use crate::{
    admin::list_config_resources::{
        ListConfigResourcesShardLockError, ListConfigResourcesShardWake,
        ListConfigResourcesShardWakeError, ListConfigResourcesTurn,
    },
    driver::{ListConfigResourcesCall, ReactorWake},
    protocol::admin::list_config_resources::list_config_resources_request,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListConfigResourcesProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListConfigResourcesProgress, EngineHostError> {
    let mut host = match resources.list_config_resources.try_host() {
        Ok(host) => host,
        Err(ListConfigResourcesShardLockError::Contended) => {
            return Ok(ListConfigResourcesProgress::contended());
        }
        Err(ListConfigResourcesShardLockError::Poisoned) => {
            return Err(EngineHostError::ListConfigResourcesLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources.list_config_resources.close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ListConfigResources)?;
    let driver_progress = match turn {
        ListConfigResourcesTurn::Idle => false,
        ListConfigResourcesTurn::Progress => true,
        ListConfigResourcesTurn::Submit(submission) => {
            let (operation_id, deadline, plan, _result_limit) = submission.into_parts();
            let Some(resource_types) = materialize_resource_types(&plan) else {
                host.reject_handoff(operation_id)
                    .map_err(EngineHostError::ListConfigResources)?;
                return Ok(ListConfigResourcesProgress {
                    unsettled: host.unsettled(),
                    driver_progress: true,
                    next_deadline: host.next_deadline(),
                });
            };
            let Ok(request) = list_config_resources_request(&resource_types) else {
                host.reject_handoff(operation_id)
                    .map_err(EngineHostError::ListConfigResources)?;
                return Ok(ListConfigResourcesProgress {
                    unsettled: host.unsettled(),
                    driver_progress: true,
                    next_deadline: host.next_deadline(),
                });
            };
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match ListConfigResourcesCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListConfigResources)?,
                Err(_rejection) => host
                    .reject_handoff(operation_id)
                    .map_err(EngineHostError::ListConfigResources)?,
            }
            true
        }
    };
    Ok(ListConfigResourcesProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

fn materialize_resource_types(plan: &ListConfigResourcesPlan) -> Option<Vec<i8>> {
    let mut resource_types = Vec::new();
    resource_types
        .try_reserve_exact(plan.resource_types().len())
        .ok()?;
    resource_types.extend(
        plan.resource_types()
            .iter()
            .map(|resource_type| resource_type.code()),
    );
    Some(resource_types)
}

impl ListConfigResourcesProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl ListConfigResourcesShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListConfigResourcesShardWakeError> {
        self.request()
            .map_err(|error| ListConfigResourcesShardWakeError::from_io(error.into_io()))
    }
}
