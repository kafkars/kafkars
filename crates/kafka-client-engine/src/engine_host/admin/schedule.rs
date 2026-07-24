//! Explicit fair sequencing of concrete admin owners.

use std::sync::Arc;

use kafka_client_core::{Deadline, Moment};

use super::{
    super::{EngineHostError, EngineHostResources},
    create_partitions, create_topics, delete_topics, describe_cluster,
};

pub(in crate::engine_host) struct AdminProgress {
    pub(in crate::engine_host) unsettled: usize,
    pub(in crate::engine_host) driver_progress: bool,
    pub(in crate::engine_host) next_deadline: Option<Deadline>,
}

pub(in crate::engine_host) fn drive(
    resources: &mut EngineHostResources,
) -> Result<AdminProgress, EngineHostError> {
    // Drive all concrete owners independently. Exhaustion or contention in
    // one tracked-call lane must not hide runnable work in the other.
    let clock = Arc::clone(&resources.clock);
    let create_now = clock.now().map_err(EngineHostError::Clock)?;
    let (create, delete_now) = drive_create_then_capture_delete(
        create_now,
        |now| create_topics::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let (delete, describe_now) = drive_delete_then_capture_describe(
        delete_now,
        |now| delete_topics::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let describe = describe_cluster::drive(resources, describe_now)?;
    let partitions_now = clock.now().map_err(EngineHostError::Clock)?;
    let partitions = create_partitions::drive(resources, partitions_now)?;
    Ok(combine(&create, &delete, &describe, &partitions))
}

pub(super) fn drive_create_then_capture_delete(
    create_now: Moment,
    drive_create: impl FnOnce(Moment) -> Result<create_topics::CreateTopicsProgress, EngineHostError>,
    capture_delete_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(create_topics::CreateTopicsProgress, Moment), EngineHostError> {
    let create = drive_create(create_now)?;
    let delete_now = capture_delete_now()?;
    Ok((create, delete_now))
}

pub(super) fn drive_delete_then_capture_describe(
    delete_now: Moment,
    drive_delete: impl FnOnce(Moment) -> Result<delete_topics::DeleteTopicsProgress, EngineHostError>,
    capture_describe_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(delete_topics::DeleteTopicsProgress, Moment), EngineHostError> {
    let delete = drive_delete(delete_now)?;
    let describe_now = capture_describe_now()?;
    Ok((delete, describe_now))
}

pub(super) const fn combine(
    create: &create_topics::CreateTopicsProgress,
    delete: &delete_topics::DeleteTopicsProgress,
    describe: &describe_cluster::DescribeClusterProgress,
    partitions: &create_partitions::CreatePartitionsProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: create
            .unsettled
            .saturating_add(delete.unsettled)
            .saturating_add(describe.unsettled)
            .saturating_add(partitions.unsettled),
        driver_progress: create.driver_progress
            || delete.driver_progress
            || describe.driver_progress
            || partitions.driver_progress,
        next_deadline: earliest(
            earliest(create.next_deadline, delete.next_deadline),
            earliest(describe.next_deadline, partitions.next_deadline),
        ),
    }
}

pub(in crate::engine_host) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let create = create_topics::apply_completions(resources)?;
    let delete = delete_topics::apply_completions(resources)?;
    let describe = describe_cluster::apply_completions(resources)?;
    let partitions = create_partitions::apply_completions(resources)?;
    Ok(create || delete || describe || partitions)
}

#[cfg(test)]
impl AdminProgress {
    pub(in crate::engine_host) const fn contended() -> Self {
        Self {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        }
    }
}

const fn earliest(left: Option<Deadline>, right: Option<Deadline>) -> Option<Deadline> {
    match (left, right) {
        (Some(left), Some(right)) if left.tick() <= right.tick() => Some(left),
        (Some(_left), Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
