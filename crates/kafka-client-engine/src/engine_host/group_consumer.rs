//! One bounded private classic-group registry turn on the embedded host.

use kafka_client_core::{Deadline, Moment};

use crate::{consumer::GroupConsumerRegistry, driver::DriverOwner};

use super::{EngineHostError, EngineHostResources};

pub(super) struct GroupConsumerProgress {
    pub(super) unsettled: usize,
    pub(super) progressed: bool,
    pub(super) next_deadline: Option<Deadline>,
}

pub(super) fn drive(
    resources: &mut EngineHostResources,
    stage_now: Moment,
) -> Result<GroupConsumerProgress, EngineHostError> {
    let driver = resources
        .driver
        .as_ref()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    drive_registry(
        &mut resources.group_consumers,
        driver,
        resources.control.shutdown_requested(),
        stage_now,
    )
}

pub(super) fn drive_registry(
    registry: &mut GroupConsumerRegistry,
    driver: &DriverOwner,
    shutdown: bool,
    stage_now: Moment,
) -> Result<GroupConsumerProgress, EngineHostError> {
    if shutdown {
        registry.close_admission();
    }
    let progressed = registry
        .turn(stage_now, driver)
        .map_err(EngineHostError::GroupConsumer)?;
    Ok(GroupConsumerProgress {
        unsettled: registry.unsettled(),
        progressed,
        next_deadline: registry.next_deadline(),
    })
}
