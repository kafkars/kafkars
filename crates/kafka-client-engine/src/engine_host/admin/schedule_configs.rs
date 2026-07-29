//! Closed aggregation and sequencing helpers for topic configuration owners.

use kafka_client_core::Deadline;

use super::{
    alter_consumer_group_offsets, create_partitions, create_topics, delete_consumer_group_offsets,
    delete_topics, describe_cluster, describe_configs, describe_topics, incremental_alter_configs,
    legacy_alter_configs, list_consumer_group_offsets, schedule::AdminProgress,
};

// The closed foundational admin-owner set stays explicit at one boundary.
#[expect(clippy::too_many_arguments)]
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

pub(super) const fn extend_with_legacy_alter_configs(
    progress: &AdminProgress,
    legacy: &legacy_alter_configs::LegacyAlterConfigsProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: progress.unsettled.saturating_add(legacy.unsettled),
        driver_progress: progress.driver_progress || legacy.driver_progress,
        next_deadline: earliest(progress.next_deadline, legacy.next_deadline),
    }
}

pub(super) const fn earliest(left: Option<Deadline>, right: Option<Deadline>) -> Option<Deadline> {
    super::schedule_deadline::earliest(left, right)
}
