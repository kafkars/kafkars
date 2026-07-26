//! Explicit fair sequencing of concrete admin owners.

use std::sync::Arc;

use kafka_client_core::{Deadline, Moment};

use super::{
    super::{EngineHostError, EngineHostResources},
    create_partitions, create_topics, delete_topics, describe_cluster, describe_configs,
    describe_topics, incremental_alter_configs, list_consumer_group_offsets,
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
    // one owner must not hide runnable work in another.
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
    let (describe, topics_now) = drive_describe_then_capture_topics(
        describe_now,
        |now| describe_cluster::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let topics = describe_topics::drive(resources, topics_now)?;
    let configs_now = clock.now().map_err(EngineHostError::Clock)?;
    let configs = describe_configs::drive(resources, configs_now)?;
    let partitions_now = clock.now().map_err(EngineHostError::Clock)?;
    let partitions = create_partitions::drive(resources, partitions_now)?;
    let alter_configs_now = clock.now().map_err(EngineHostError::Clock)?;
    let (alter_configs, group_offsets_now) = drive_alter_configs_then_capture_group_offsets(
        alter_configs_now,
        |now| incremental_alter_configs::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let group_offsets = list_consumer_group_offsets::drive(resources, group_offsets_now)?;
    Ok(combine(
        &create,
        &delete,
        &describe,
        &partitions,
        &topics,
        &configs,
        &alter_configs,
        &group_offsets,
    ))
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

pub(super) fn drive_describe_then_capture_topics(
    describe_now: Moment,
    drive_describe: impl FnOnce(
        Moment,
    )
        -> Result<describe_cluster::DescribeClusterProgress, EngineHostError>,
    capture_topics_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(describe_cluster::DescribeClusterProgress, Moment), EngineHostError> {
    let describe = drive_describe(describe_now)?;
    let topics_now = capture_topics_now()?;
    Ok((describe, topics_now))
}

pub(super) fn drive_alter_configs_then_capture_group_offsets(
    alter_configs_now: Moment,
    drive_alter_configs: impl FnOnce(
        Moment,
    ) -> Result<
        incremental_alter_configs::IncrementalAlterConfigsProgress,
        EngineHostError,
    >,
    capture_group_offsets_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<
    (
        incremental_alter_configs::IncrementalAlterConfigsProgress,
        Moment,
    ),
    EngineHostError,
> {
    let alter_configs = drive_alter_configs(alter_configs_now)?;
    let group_offsets_now = capture_group_offsets_now()?;
    Ok((alter_configs, group_offsets_now))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the fixed closed admin-owner set stays explicit at its only aggregation boundary"
)]
pub(super) const fn combine(
    create: &create_topics::CreateTopicsProgress,
    delete: &delete_topics::DeleteTopicsProgress,
    describe: &describe_cluster::DescribeClusterProgress,
    partitions: &create_partitions::CreatePartitionsProgress,
    topics: &describe_topics::DescribeTopicsProgress,
    configs: &describe_configs::DescribeConfigsProgress,
    alter_configs: &incremental_alter_configs::IncrementalAlterConfigsProgress,
    group_offsets: &list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: create
            .unsettled
            .saturating_add(delete.unsettled)
            .saturating_add(describe.unsettled)
            .saturating_add(partitions.unsettled)
            .saturating_add(topics.unsettled)
            .saturating_add(configs.unsettled)
            .saturating_add(alter_configs.unsettled)
            .saturating_add(group_offsets.unsettled),
        driver_progress: create.driver_progress
            || delete.driver_progress
            || describe.driver_progress
            || partitions.driver_progress
            || topics.driver_progress
            || configs.driver_progress
            || alter_configs.driver_progress
            || group_offsets.driver_progress,
        next_deadline: earliest(
            earliest(create.next_deadline, delete.next_deadline),
            earliest(
                earliest(describe.next_deadline, partitions.next_deadline),
                earliest(
                    earliest(topics.next_deadline, configs.next_deadline),
                    earliest(alter_configs.next_deadline, group_offsets.next_deadline),
                ),
            ),
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
    let topics = describe_topics::apply_completions(resources)?;
    let configs = describe_configs::apply_completions(resources)?;
    let alter_configs = incremental_alter_configs::apply_completions(resources)?;
    Ok(create || delete || describe || partitions || topics || configs || alter_configs)
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
