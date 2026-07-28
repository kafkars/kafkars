//! Ordered post-driver recovery for concrete admin operation owners.

mod partition;

use super::super::{EngineHostError, EngineHostResources};
use partition::recover_partition_operations;

pub(in crate::engine_host) fn recover_operations(
    resources: &EngineHostResources,
    failure: EngineHostError,
) -> EngineHostError {
    let failure = recover_topic_operations(resources, failure);
    let failure = recover_group_operations(resources, failure);
    recover_partition_operations(resources, failure)
}

fn recover_topic_operations(
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
    let mut describe_acls = resources.describe_acls.terminal_host();
    if let Some(cleanup) = describe_acls
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeAcls)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_acls);
    let mut describe_client_quotas = resources.describe_client_quotas.terminal_host();
    if let Some(cleanup) = describe_client_quotas
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeClientQuotas)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_client_quotas);
    let mut create_acls = resources.create_acls.terminal_host();
    if let Some(cleanup) = create_acls
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreateAcls)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_acls);
    let mut delete_acls = resources.delete_acls.terminal_host();
    if let Some(cleanup) = delete_acls
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteAcls)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_acls);
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
    failure
}

fn recover_group_operations(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
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
    failure
}
