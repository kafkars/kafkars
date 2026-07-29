//! Closed broker-operation progress aggregation for the fair admin scheduler.

use super::{
    add_raft_voter, alter_client_quotas, alter_replica_log_dirs, alter_user_scram_credentials,
    create_acls, create_delegation_token, delete_acls, describe_acls, describe_client_quotas,
    describe_delegation_tokens, describe_features, describe_log_dirs, describe_metadata_quorum,
    describe_replica_log_dirs, describe_user_scram_credentials, expire_delegation_token,
    list_client_metrics_resources, list_config_resources, remove_raft_voter,
    renew_delegation_token, schedule::AdminProgress, schedule_deadline::earliest,
    unregister_broker, update_features,
};

// The broker-operation owner set stays explicit at its sole aggregation boundary.
#[expect(clippy::too_many_arguments)]
pub(super) const fn extend_with_broker_operations(
    progress: &AdminProgress,
    log_directories: &describe_log_dirs::DescribeLogDirsProgress,
    replica_log_directories: &describe_replica_log_dirs::DescribeReplicaLogDirsProgress,
    log_directory_alterations: &alter_replica_log_dirs::AlterReplicaLogDirsProgress,
    acl_descriptions: &describe_acls::DescribeAclsProgress,
    quota_descriptions: &describe_client_quotas::DescribeClientQuotasProgress,
    quota_alterations: &alter_client_quotas::AlterClientQuotasProgress,
    scram_alterations: &alter_user_scram_credentials::AlterUserScramCredentialsProgress,
    scram_descriptions: &describe_user_scram_credentials::DescribeUserScramCredentialsProgress,
    metadata_quorum: &describe_metadata_quorum::DescribeMetadataQuorumProgress,
    feature_updates: &update_features::UpdateFeaturesProgress,
    feature_descriptions: &describe_features::DescribeFeaturesProgress,
    broker_unregistrations: &unregister_broker::UnregisterBrokerProgress,
    voter_additions: &add_raft_voter::AddRaftVoterProgress,
    voter_removals: &remove_raft_voter::RemoveRaftVoterProgress,
    client_metrics_resources: &list_client_metrics_resources::ListClientMetricsResourcesProgress,
    config_resources: &list_config_resources::ListConfigResourcesProgress,
    acl_creations: &create_acls::CreateAclsProgress,
    acl_deletions: &delete_acls::DeleteAclsProgress,
    delegation_token_creations: &create_delegation_token::CreateDelegationTokenProgress,
    delegation_token_descriptions: &describe_delegation_tokens::DescribeDelegationTokensProgress,
    delegation_token_renewals: &renew_delegation_token::RenewDelegationTokenProgress,
    delegation_token_expirations: &expire_delegation_token::ExpireDelegationTokenProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: progress
            .unsettled
            .saturating_add(log_directories.unsettled)
            .saturating_add(replica_log_directories.unsettled)
            .saturating_add(log_directory_alterations.unsettled)
            .saturating_add(acl_descriptions.unsettled)
            .saturating_add(quota_descriptions.unsettled)
            .saturating_add(quota_alterations.unsettled)
            .saturating_add(scram_alterations.unsettled)
            .saturating_add(scram_descriptions.unsettled)
            .saturating_add(metadata_quorum.unsettled)
            .saturating_add(feature_updates.unsettled)
            .saturating_add(feature_descriptions.unsettled)
            .saturating_add(broker_unregistrations.unsettled)
            .saturating_add(voter_additions.unsettled)
            .saturating_add(voter_removals.unsettled)
            .saturating_add(client_metrics_resources.unsettled)
            .saturating_add(config_resources.unsettled)
            .saturating_add(acl_creations.unsettled)
            .saturating_add(acl_deletions.unsettled)
            .saturating_add(delegation_token_creations.unsettled)
            .saturating_add(delegation_token_descriptions.unsettled)
            .saturating_add(delegation_token_renewals.unsettled)
            .saturating_add(delegation_token_expirations.unsettled),
        driver_progress: progress.driver_progress
            || log_directories.driver_progress
            || replica_log_directories.driver_progress
            || log_directory_alterations.driver_progress
            || acl_descriptions.driver_progress
            || quota_descriptions.driver_progress
            || quota_alterations.driver_progress
            || scram_alterations.driver_progress
            || scram_descriptions.driver_progress
            || metadata_quorum.driver_progress
            || feature_updates.driver_progress
            || feature_descriptions.driver_progress
            || broker_unregistrations.driver_progress
            || voter_additions.driver_progress
            || voter_removals.driver_progress
            || client_metrics_resources.driver_progress
            || config_resources.driver_progress
            || acl_creations.driver_progress
            || acl_deletions.driver_progress
            || delegation_token_creations.driver_progress
            || delegation_token_descriptions.driver_progress
            || delegation_token_renewals.driver_progress
            || delegation_token_expirations.driver_progress,
        next_deadline: earliest(
            progress.next_deadline,
            earliest(
                metadata_quorum.next_deadline,
                earliest(
                    feature_updates.next_deadline,
                    earliest(
                        feature_descriptions.next_deadline,
                        earliest(
                            broker_unregistrations.next_deadline,
                            earliest(
                                voter_additions.next_deadline,
                                earliest(
                                    voter_removals.next_deadline,
                                    earliest(
                                        client_metrics_resources.next_deadline,
                                        earliest(
                                            config_resources.next_deadline,
                                            earliest(
                                                log_directories.next_deadline,
                                                earliest(
                                                    replica_log_directories.next_deadline,
                                                    earliest(
                                                        log_directory_alterations.next_deadline,
                                                        earliest(
                                                            acl_descriptions.next_deadline,
                                                            earliest(
                                                                quota_descriptions.next_deadline,
                                                                earliest(
                                                                    quota_alterations.next_deadline,
                                                                    earliest(
                                                                        scram_alterations
                                                                            .next_deadline,
                                                                        earliest(
                                                                            scram_descriptions
                                                                                .next_deadline,
                                                                            earliest(
                                                                                acl_creations
                                                                                    .next_deadline,
                                                                                earliest(
                                                                                    acl_deletions
                                                                                        .next_deadline,
                                                                                    earliest(
                                                                                        delegation_token_creations
                                                                                            .next_deadline,
                                                                                        earliest(
                                                                                            delegation_token_descriptions
                                                                                                .next_deadline,
                                                                                            earliest(
                                                                                                delegation_token_renewals
                                                                                                    .next_deadline,
                                                                                                delegation_token_expirations
                                                                                                    .next_deadline,
                                                                                            ),
                                                                                        ),
                                                                                    ),
                                                                                ),
                                                                            ),
                                                                        ),
                                                                    ),
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    }
}
