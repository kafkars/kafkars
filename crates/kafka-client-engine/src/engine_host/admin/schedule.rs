//! Explicit fair sequencing of concrete admin owners.

use std::sync::Arc;

use kafka_client_core::{Deadline, Moment};

use super::{
    super::{EngineHostError, EngineHostResources},
    alter_consumer_group_offsets, alter_partition_reassignments, alter_replica_log_dirs,
    create_partitions, create_topics, delete_consumer_group_offsets, delete_topics,
    describe_cluster, describe_configs, describe_topics,
    group_offset_alter_schedule::drive_group_offset_delete_then_capture_alter,
    incremental_alter_configs, list_consumer_group_offsets, list_offsets, list_offsets_schedule,
    list_partition_reassignments,
    schedule_deadline::earliest,
};

pub(in crate::engine_host) struct AdminProgress {
    pub(in crate::engine_host) unsettled: usize,
    pub(in crate::engine_host) driver_progress: bool,
    pub(in crate::engine_host) next_deadline: Option<Deadline>,
}

pub(in crate::engine_host) fn drive(
    resources: &mut EngineHostResources,
) -> Result<AdminProgress, EngineHostError> {
    // Contention in one concrete owner must not hide runnable work in another.
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
    let (group_offsets, group_offset_delete_now) = drive_group_offsets_then_capture_delete(
        group_offsets_now,
        |now| list_consumer_group_offsets::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let (group_offset_delete, group_offset_alter_now) =
        drive_group_offset_delete_then_capture_alter(
            group_offset_delete_now,
            |now| delete_consumer_group_offsets::drive(resources, now),
            || clock.now().map_err(EngineHostError::Clock),
        )?;
    let group_offset_alter =
        alter_consumer_group_offsets::drive(resources, group_offset_alter_now)?;
    let list_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let list_offsets_progress = list_offsets::drive(resources, list_offsets_now)?;
    let mut progress = combine(
        &create,
        &delete,
        &describe,
        &partitions,
        &topics,
        &configs,
        &alter_configs,
        &group_offsets,
        &group_offset_delete,
        &group_offset_alter,
    );
    list_offsets_schedule::extend(&mut progress, &list_offsets_progress);
    let listing_now = clock.now().map_err(EngineHostError::Clock)?;
    let listing = list_partition_reassignments::drive(resources, listing_now)?;
    list_offsets_schedule::extend_partition_reassignments(&mut progress, &listing);
    let alteration_now = clock.now().map_err(EngineHostError::Clock)?;
    let alteration = alter_partition_reassignments::drive(resources, alteration_now)?;
    list_offsets_schedule::extend_partition_reassignment_alterations(&mut progress, &alteration);
    let alter_log_dirs_now = clock.now().map_err(EngineHostError::Clock)?;
    let log_directory_alterations = alter_replica_log_dirs::drive(resources, alter_log_dirs_now)?;
    Ok(extend_with_alter_replica_log_dirs(
        &progress,
        &log_directory_alterations,
    ))
}

const fn extend_with_alter_replica_log_dirs(
    progress: &AdminProgress,
    log_directory_alterations: &alter_replica_log_dirs::AlterReplicaLogDirsProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: progress
            .unsettled
            .saturating_add(log_directory_alterations.unsettled),
        driver_progress: progress.driver_progress || log_directory_alterations.driver_progress,
        next_deadline: earliest(
            progress.next_deadline,
            log_directory_alterations.next_deadline,
        ),
    }
}

pub(super) fn drive_group_offsets_then_capture_delete(
    group_offsets_now: Moment,
    drive_group_offsets: impl FnOnce(
        Moment,
    ) -> Result<
        list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
        EngineHostError,
    >,
    capture_delete_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<
    (
        list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
        Moment,
    ),
    EngineHostError,
> {
    let group_offsets = drive_group_offsets(group_offsets_now)?;
    let delete_now = capture_delete_now()?;
    Ok((group_offsets, delete_now))
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
    group_offset_delete: &delete_consumer_group_offsets::DeleteConsumerGroupOffsetsProgress,
    group_offset_alter: &alter_consumer_group_offsets::AlterConsumerGroupOffsetsProgress,
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
            .saturating_add(group_offsets.unsettled)
            .saturating_add(group_offset_delete.unsettled)
            .saturating_add(group_offset_alter.unsettled),
        driver_progress: create.driver_progress
            || delete.driver_progress
            || describe.driver_progress
            || partitions.driver_progress
            || topics.driver_progress
            || configs.driver_progress
            || alter_configs.driver_progress
            || group_offsets.driver_progress
            || group_offset_delete.driver_progress
            || group_offset_alter.driver_progress,
        next_deadline: earliest(
            earliest(create.next_deadline, delete.next_deadline),
            earliest(
                earliest(describe.next_deadline, partitions.next_deadline),
                earliest(
                    earliest(topics.next_deadline, configs.next_deadline),
                    earliest(
                        earliest(alter_configs.next_deadline, group_offsets.next_deadline),
                        earliest(
                            group_offset_delete.next_deadline,
                            group_offset_alter.next_deadline,
                        ),
                    ),
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
