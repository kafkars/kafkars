//! Ordered post-driver recovery for concrete admin operation owners.

mod add_raft_voter;
mod client_metrics_resources;
mod config_resources;
mod describe_features;
mod group;
mod metadata_quorum;
mod partition;
mod remove_raft_voter;
mod unregister_broker;
mod update_features;

use super::super::{EngineHostError, EngineHostResources};
use partition::recover_partition_operations;

pub(in crate::engine_host) fn recover_operations(
    resources: &EngineHostResources,
    failure: EngineHostError,
) -> EngineHostError {
    let failure = recover_topic_operations(resources, failure);
    let failure = group::recover(resources, failure);
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
    let mut describe_replica_log_dirs = resources.describe_replica_log_dirs.terminal_host();
    if let Some(cleanup) = describe_replica_log_dirs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeReplicaLogDirs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_replica_log_dirs);
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
    let mut alter_client_quotas = resources.alter_client_quotas.terminal_host();
    if let Some(cleanup) = alter_client_quotas
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterClientQuotas)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_client_quotas);
    let mut alter_user_scram_credentials = resources.alter_user_scram_credentials.terminal_host();
    if let Some(cleanup) = alter_user_scram_credentials
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AlterUserScramCredentials)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(alter_user_scram_credentials);
    let mut create_delegation_token = resources.create_delegation_token.terminal_host();
    if let Some(cleanup) = create_delegation_token
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreateDelegationToken)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_delegation_token);
    let mut describe_delegation_tokens = resources.describe_delegation_tokens.terminal_host();
    if let Some(cleanup) = describe_delegation_tokens
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeDelegationTokens)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_delegation_tokens);
    let mut renew_delegation_token = resources.renew_delegation_token.terminal_host();
    if let Some(cleanup) = renew_delegation_token
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::RenewDelegationToken)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(renew_delegation_token);
    let mut expire_delegation_token = resources.expire_delegation_token.terminal_host();
    if let Some(cleanup) = expire_delegation_token
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::ExpireDelegationToken)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(expire_delegation_token);
    failure = update_features::recover(resources, failure);
    failure = describe_features::recover(resources, failure);
    failure = unregister_broker::recover(resources, failure);
    failure = add_raft_voter::recover(resources, failure);
    failure = remove_raft_voter::recover(resources, failure);
    let mut describe_user_scram_credentials =
        resources.describe_user_scram_credentials.terminal_host();
    if let Some(cleanup) = describe_user_scram_credentials
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeUserScramCredentials)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_user_scram_credentials);
    failure = metadata_quorum::recover(resources, failure);
    failure = client_metrics_resources::recover(resources, failure);
    failure = config_resources::recover(resources, failure);
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
    let mut legacy_alter_configs = resources.legacy_alter_configs.terminal_host();
    if let Some(cleanup) = legacy_alter_configs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::LegacyAlterConfigs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(legacy_alter_configs);
    failure
}
