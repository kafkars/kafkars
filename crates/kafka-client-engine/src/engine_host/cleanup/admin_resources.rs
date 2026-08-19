//! Terminal verification for concrete resource- and cluster-scoped Admin owners.

use super::super::{EngineHostError, EngineHostResources};

#[allow(
    clippy::too_many_lines,
    reason = "terminal verification names every resource-scoped owner and its exact error"
)]
pub(super) fn verify(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let create = resources.create_topics.terminal_host().unsettled();
    if create != 0 {
        return Err(EngineHostError::CreateTopics(
            crate::admin::CreateTopicsHostError::Unsettled(create),
        ));
    }
    let delete = resources.delete_topics.terminal_host().unsettled();
    if delete != 0 {
        return Err(EngineHostError::DeleteTopics(
            crate::admin::DeleteTopicsHostError::Unsettled(delete),
        ));
    }
    let describe = resources.describe_cluster.terminal_host().unsettled();
    if describe != 0 {
        return Err(EngineHostError::DescribeCluster(
            crate::admin::DescribeClusterHostError::Unsettled(describe),
        ));
    }
    let described_groups = resources
        .describe_consumer_groups
        .terminal_host()
        .unsettled();
    if described_groups != 0 {
        return Err(EngineHostError::DescribeConsumerGroups(
            crate::admin::DescribeConsumerGroupsHostError::Unsettled(described_groups),
        ));
    }
    let described_log_dirs = resources.describe_log_dirs.terminal_host().unsettled();
    if described_log_dirs != 0 {
        return Err(EngineHostError::DescribeLogDirs(
            crate::admin::DescribeLogDirsHostError::Unsettled(described_log_dirs),
        ));
    }
    let described_replica_log_dirs = resources
        .describe_replica_log_dirs
        .terminal_host()
        .unsettled();
    if described_replica_log_dirs != 0 {
        return Err(EngineHostError::DescribeReplicaLogDirs(
            crate::admin::DescribeReplicaLogDirsHostError::Unsettled(described_replica_log_dirs),
        ));
    }
    let altered_log_dirs = resources.alter_replica_log_dirs.terminal_host().unsettled();
    if altered_log_dirs != 0 {
        return Err(EngineHostError::AlterReplicaLogDirs(
            crate::admin::AlterReplicaLogDirsHostError::Unsettled(altered_log_dirs),
        ));
    }
    let described_acls = resources.describe_acls.terminal_host().unsettled();
    if described_acls != 0 {
        return Err(EngineHostError::DescribeAcls(
            crate::admin::DescribeAclsHostError::Unsettled(described_acls),
        ));
    }
    let described_client_quotas = resources.describe_client_quotas.terminal_host().unsettled();
    if described_client_quotas != 0 {
        return Err(EngineHostError::DescribeClientQuotas(
            crate::admin::DescribeClientQuotasHostError::Unsettled(described_client_quotas),
        ));
    }
    let altered_client_quotas = resources.alter_client_quotas.terminal_host().unsettled();
    if altered_client_quotas != 0 {
        return Err(EngineHostError::AlterClientQuotas(
            crate::admin::AlterClientQuotasHostError::Unsettled(altered_client_quotas),
        ));
    }
    let altered_scram_credentials = resources
        .alter_user_scram_credentials
        .terminal_host()
        .unsettled();
    if altered_scram_credentials != 0 {
        return Err(EngineHostError::AlterUserScramCredentials(
            crate::admin::AlterUserScramCredentialsHostError::Unsettled(altered_scram_credentials),
        ));
    }
    let updated_features = resources.update_features.terminal_host().unsettled();
    if updated_features != 0 {
        return Err(EngineHostError::UpdateFeatures(
            crate::admin::update_features::UpdateFeaturesHostError::Unsettled(updated_features),
        ));
    }
    let described_features = resources.describe_features.terminal_host().unsettled();
    if described_features != 0 {
        return Err(EngineHostError::DescribeFeatures(
            crate::admin::DescribeFeaturesHostError::Unsettled(described_features),
        ));
    }
    let unregistered_brokers = resources.unregister_broker.terminal_host().unsettled();
    if unregistered_brokers != 0 {
        return Err(EngineHostError::UnregisterBroker(
            crate::admin::unregister_broker::UnregisterBrokerHostError::Unsettled(
                unregistered_brokers,
            ),
        ));
    }
    let added_raft_voters = resources.add_raft_voter.terminal_host().unsettled();
    if added_raft_voters != 0 {
        return Err(EngineHostError::AddRaftVoter(
            crate::admin::AddRaftVoterHostError::Unsettled(added_raft_voters),
        ));
    }
    let removed_raft_voters = resources.remove_raft_voter.terminal_host().unsettled();
    if removed_raft_voters != 0 {
        return Err(EngineHostError::RemoveRaftVoter(
            crate::admin::RemoveRaftVoterHostError::Unsettled(removed_raft_voters),
        ));
    }
    let described_scram_credentials = resources
        .describe_user_scram_credentials
        .terminal_host()
        .unsettled();
    if described_scram_credentials != 0 {
        return Err(EngineHostError::DescribeUserScramCredentials(
            crate::admin::DescribeUserScramCredentialsHostError::Unsettled(
                described_scram_credentials,
            ),
        ));
    }
    let described_metadata_quorum = resources
        .describe_metadata_quorum
        .terminal_host()
        .unsettled();
    if described_metadata_quorum != 0 {
        return Err(EngineHostError::DescribeMetadataQuorum(
            crate::admin::DescribeMetadataQuorumHostError::Unsettled(described_metadata_quorum),
        ));
    }
    let created_acls = resources.create_acls.terminal_host().unsettled();
    if created_acls != 0 {
        return Err(EngineHostError::CreateAcls(
            crate::admin::CreateAclsHostError::Unsettled(created_acls),
        ));
    }
    let created_delegation_tokens = resources
        .create_delegation_token
        .terminal_host()
        .unsettled();
    if created_delegation_tokens != 0 {
        return Err(EngineHostError::CreateDelegationToken(
            crate::admin::CreateDelegationTokenHostError::Unsettled(created_delegation_tokens),
        ));
    }
    let described_delegation_tokens = resources
        .describe_delegation_tokens
        .terminal_host()
        .unsettled();
    if described_delegation_tokens != 0 {
        return Err(EngineHostError::DescribeDelegationTokens(
            crate::admin::DescribeDelegationTokensHostError::Unsettled(described_delegation_tokens),
        ));
    }
    let renewed_delegation_tokens = resources.renew_delegation_token.terminal_host().unsettled();
    if renewed_delegation_tokens != 0 {
        return Err(EngineHostError::RenewDelegationToken(
            crate::admin::RenewDelegationTokenHostError::Unsettled(renewed_delegation_tokens),
        ));
    }
    let expired_delegation_tokens = resources
        .expire_delegation_token
        .terminal_host()
        .unsettled();
    if expired_delegation_tokens != 0 {
        return Err(EngineHostError::ExpireDelegationToken(
            crate::admin::ExpireDelegationTokenHostError::Unsettled(expired_delegation_tokens),
        ));
    }
    let deleted_acls = resources.delete_acls.terminal_host().unsettled();
    if deleted_acls != 0 {
        return Err(EngineHostError::DeleteAcls(
            crate::admin::DeleteAclsHostError::Unsettled(deleted_acls),
        ));
    }
    let partitions = resources.create_partitions.terminal_host().unsettled();
    if partitions != 0 {
        return Err(EngineHostError::CreatePartitions(
            crate::admin::CreatePartitionsHostError::Unsettled(partitions),
        ));
    }
    let topics = resources.describe_topics.terminal_host().unsettled();
    if topics != 0 {
        return Err(EngineHostError::DescribeTopics(
            crate::admin::DescribeTopicsHostError::Unsettled(topics),
        ));
    }
    let configs = resources.describe_configs.terminal_host().unsettled();
    if configs != 0 {
        return Err(EngineHostError::DescribeConfigs(
            crate::admin::DescribeConfigsHostError::Unsettled(configs),
        ));
    }
    let alter_configs = resources
        .incremental_alter_configs
        .terminal_host()
        .unsettled();
    if alter_configs != 0 {
        return Err(EngineHostError::IncrementalAlterConfigs(
            crate::admin::IncrementalAlterConfigsHostError::Unsettled(alter_configs),
        ));
    }
    let legacy_configs = resources.legacy_alter_configs.terminal_host().unsettled();
    if legacy_configs != 0 {
        return Err(EngineHostError::LegacyAlterConfigs(
            crate::admin::LegacyAlterConfigsHostError::Unsettled(legacy_configs),
        ));
    }
    Ok(())
}
