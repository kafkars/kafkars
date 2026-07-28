//! Ordered post-driver recovery for concrete admin operation owners.

use super::super::{EngineHostError, EngineHostResources};

pub(in crate::engine_host) fn recover_operations(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut create_topics = resources.create_topics.terminal_host();
    if let Some(cleanup) = create_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreateTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_topics);
    let mut delete_topics = resources.delete_topics.terminal_host();
    if let Some(cleanup) = delete_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_topics);
    let mut describe_cluster = resources.describe_cluster.terminal_host();
    if let Some(cleanup) = describe_cluster
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeCluster)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_cluster);
    let mut create_partitions = resources.create_partitions.terminal_host();
    if let Some(cleanup) = create_partitions
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreatePartitions)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_partitions);
    let mut describe_topics = resources.describe_topics.terminal_host();
    if let Some(cleanup) = describe_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_topics);
    let mut describe_configs = resources.describe_configs.terminal_host();
    if let Some(cleanup) = describe_configs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeConfigs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_configs);
    let mut incremental_alter_configs = resources.incremental_alter_configs.terminal_host();
    if let Some(cleanup) = incremental_alter_configs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::IncrementalAlterConfigs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(incremental_alter_configs);
    let mut list_consumer_group_offsets = resources.list_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = list_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_consumer_group_offsets);
    let mut list_consumer_groups = resources.list_consumer_groups.terminal_host();
    if let Some(cleanup) = list_consumer_groups
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListConsumerGroups)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_consumer_groups);
    let mut delete_consumer_group_offsets = resources.delete_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = delete_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_consumer_group_offsets);
    let mut delete_consumer_groups = resources.delete_consumer_groups.terminal_host();
    if let Some(cleanup) = delete_consumer_groups
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteConsumerGroups)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_consumer_groups);
    let mut alter_consumer_group_offsets = resources.alter_consumer_group_offsets.terminal_host();
    if let Some(cleanup) = alter_consumer_group_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterConsumerGroupOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_consumer_group_offsets);
    let mut list_offsets = resources.list_offsets.terminal_host();
    if let Some(cleanup) = list_offsets
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AdminListOffsets)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(list_offsets);
    recover_listing(resources, failure)
}

fn recover_listing(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut listing = resources.list_partition_reassignments.terminal_host();
    if let Some(cleanup) = listing
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ListPartitionReassignments)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(listing);
    let mut alteration = resources.alter_partition_reassignments.terminal_host();
    if let Some(cleanup) = alteration
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterPartitionReassignments)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alteration);
    let mut elect_leaders = resources.elect_leaders.terminal_host();
    if let Some(cleanup) = elect_leaders
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ElectLeaders)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(elect_leaders);
    let mut remove_consumer_group_members = resources.remove_consumer_group_members.terminal_host();
    if let Some(cleanup) = remove_consumer_group_members
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::RemoveConsumerGroupMembers)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(remove_consumer_group_members);
    let mut describe_consumer_groups = resources.describe_consumer_groups.terminal_host();
    if let Some(cleanup) = describe_consumer_groups
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeConsumerGroups)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_consumer_groups);
    let mut describe_log_dirs = resources.describe_log_dirs.terminal_host();
    if let Some(cleanup) = describe_log_dirs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeLogDirs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_log_dirs);
    let mut alter_replica_log_dirs = resources.alter_replica_log_dirs.terminal_host();
    if let Some(cleanup) = alter_replica_log_dirs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterReplicaLogDirs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_replica_log_dirs);
    let mut delete_records = resources.delete_records.terminal_host();
    if let Some(cleanup) = delete_records
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteRecords)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_records);
    failure
}
