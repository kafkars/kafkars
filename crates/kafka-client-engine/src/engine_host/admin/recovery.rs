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
    failure
}
