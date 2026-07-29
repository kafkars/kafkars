//! Fair host turns for AnyBroker Admin client-metrics resource listing.

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::list_client_metrics_resources::internal_api::{
        ListClientMetricsResourcesShardLockError, ListClientMetricsResourcesShardWake,
        ListClientMetricsResourcesShardWakeError, ListClientMetricsResourcesTurn,
    },
    driver::{ListClientMetricsResourcesCall, ReactorWake},
    protocol::admin::list_client_metrics_resources::list_client_metrics_resources_request,
};

use super::super::{EngineHostError, EngineHostResources};

pub(super) struct ListClientMetricsResourcesProgress {
    pub(super) unsettled: usize,
    pub(super) driver_progress: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    now: Moment,
) -> Result<ListClientMetricsResourcesProgress, EngineHostError> {
    let mut host = match resources.list_client_metrics_resources.try_host() {
        Ok(host) => host,
        Err(ListClientMetricsResourcesShardLockError::Contended) => {
            return Ok(ListClientMetricsResourcesProgress::contended());
        }
        Err(ListClientMetricsResourcesShardLockError::Poisoned) => {
            return Err(EngineHostError::ListClientMetricsResourcesLockPoisoned);
        }
    };
    if resources.control.shutdown_requested() {
        resources
            .list_client_metrics_resources
            .close_locked(&mut host);
    }
    let turn = host
        .turn(now)
        .map_err(EngineHostError::ListClientMetricsResources)?;
    let driver_progress = match turn {
        ListClientMetricsResourcesTurn::Idle => false,
        ListClientMetricsResourcesTurn::Progress => true,
        ListClientMetricsResourcesTurn::Submit(submission) => {
            let (operation_id, deadline, _result_limit) = submission.into_parts();
            let request = list_client_metrics_resources_request();
            let driver = resources
                .driver
                .as_ref()
                .ok_or(EngineHostError::DriverOwnerMissing)?;
            match ListClientMetricsResourcesCall::submit(driver, request, deadline.transport()) {
                Ok(call) => host
                    .accept_call(operation_id, call)
                    .map_err(EngineHostError::ListClientMetricsResources)?,
                Err(rejection) => {
                    let _rejection = rejection;
                    host.reject_handoff(operation_id)
                        .map_err(EngineHostError::ListClientMetricsResources)?;
                }
            }
            true
        }
    };
    Ok(ListClientMetricsResourcesProgress {
        unsettled: host.unsettled(),
        driver_progress,
        next_deadline: host.next_deadline(),
    })
}

impl ListClientMetricsResourcesProgress {
    const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

impl ListClientMetricsResourcesShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListClientMetricsResourcesShardWakeError> {
        self.request()
            .map_err(|error| ListClientMetricsResourcesShardWakeError::from_io(error.into_io()))
    }
}
