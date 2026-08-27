//! Leak-free, rollback-ordered resource handoff into one self-cleaning native host.
mod admin_hosts;
mod alter_partition_reassignments;
mod consumer_registries;
mod list_offsets;
mod list_partition_reassignments;
mod share_group_offsets;
use super::{
    EngineHostControl, EngineHostResources, EngineLifecycle, EngineStartError,
    alter_consumer_group_offsets_start, assigned_consumer_start, describe_configs_start,
    notifier_start, start_handoff::StartedEngineHost, transaction_start,
};
use crate::{
    EngineConfig,
    admin::{
        AbortPartitionTransactionShardOwner, AddRaftVoterShardOwner, AdminCompletionNotifier,
        AdminDescribeProducersShardOwner, AdminDescribeTopicPartitionsShardOwner,
        AdminDescribeTransactionsShardOwner, AdminFenceProducersShardOwner,
        AdminListTransactionsShardOwner, AlterClientQuotasShardOwner,
        AlterReplicaLogDirsShardOwner, AlterUserScramCredentialsShardOwner, CreateAclsShardOwner,
        CreateDelegationTokenShardOwner, CreatePartitionsShardOwner, CreateTopicsShardOwner,
        DeleteAclsShardOwner, DeleteConsumerGroupOffsetsShardOwner, DeleteConsumerGroupsShardOwner,
        DeleteRecordsShardOwner, DeleteTopicsShardOwner, DescribeAclsShardOwner,
        DescribeClientQuotasShardOwner, DescribeClusterShardOwner,
        DescribeConsumerGroupsShardOwner, DescribeDelegationTokensShardOwner,
        DescribeFeaturesShardOwner, DescribeLogDirsShardOwner, DescribeMetadataQuorumShardOwner,
        DescribeReplicaLogDirsShardOwner, DescribeTopicsShardOwner,
        DescribeUserScramCredentialsShardOwner, ElectLeadersShardOwner,
        ExpireDelegationTokenShardOwner, ListConsumerGroupOffsetsShardOwner,
        ListConsumerGroupsShardOwner, RemoveConsumerGroupMembersShardOwner,
        RemoveRaftVoterShardOwner, RenewDelegationTokenShardOwner,
        list_client_metrics_resources::internal_api::ListClientMetricsResourcesShardOwner,
        list_config_resources::ListConfigResourcesShardOwner,
        unregister_broker::UnregisterBrokerShardOwner, update_features::UpdateFeaturesShardOwner,
    },
    clock::MonotonicClock,
    config::ValidatedEngineConfig,
    consumer::{GroupConsumerShardOwner, ShareConsumerShardOwner as ShareShardOwner},
    driver::DriverOwner,
    producer::ingress::ProducerShardOwner,
};
use std::sync::Arc;

#[allow(clippy::too_many_lines)]
pub(super) fn prepare(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
    lifecycle: &Arc<EngineLifecycle>,
) -> Result<(EngineHostResources, StartedEngineHost), EngineStartError> {
    let driver = DriverOwner::build_with_security(config, validated.security)
        .map_err(|error| EngineStartError::driver(&error))?;
    let metrics = driver.observation_handle();
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(driver.reactor_wake());
    let control = Arc::new(EngineHostControl::new(wake.as_ref().clone()));
    let (mut group_consumers, share_consumers) = consumer_registries::start()?;
    let (mut assigned_consumer_notifier, assigned_publishers) =
        notifier_start::start_assigned_consumer_notifier()?;
    let (assigned_consumer_owner, assigned_consumer) =
        match assigned_consumer_start::start_assigned_consumer(
            config.assigned_consumer_read_isolation().core(),
            validated.assigned_consumer_fetch,
            validated.assigned_consumer_limits,
            Arc::clone(&clock),
            Arc::clone(&wake),
            assigned_publishers.close,
            assigned_publishers.recv,
            assigned_publishers.event,
        ) {
            Ok(owner) => owner,
            Err(error) => {
                notifier_start::join_acquired(assigned_consumer_notifier.take_join());
                return Err(EngineStartError::assigned_consumer(error));
            }
        };
    let (mut admin_notifier, admin_ports) = match AdminCompletionNotifier::start() {
        Ok(owner) => owner,
        Err(error) => {
            notifier_start::join_acquired(assigned_consumer_notifier.take_join());
            return Err(EngineStartError::admin_notifier(&error));
        }
    };
    let admin_hosts::StartedAdminHosts {
        abort_partition_transaction,
        add_raft_voter,
        remove_raft_voter,
        create_topics,
        create_acls,
        create_delegation_token,
        describe_delegation_tokens,
        renew_delegation_token,
        expire_delegation_token,
        delete_acls,
        delete_topics,
        delete_consumer_groups,
        delete_records,
        describe_acls,
        describe_client_quotas,
        alter_client_quotas,
        alter_user_scram_credentials,
        update_features,
        unregister_broker,
        describe_user_scram_credentials,
        describe_metadata_quorum,
        describe_producers,
        describe_topic_partitions,
        describe_transactions,
        fence_producers,
        list_transactions,
        list_client_metrics_resources,
        list_config_resources,
        describe_cluster,
        describe_consumer_groups,
        describe_features,
        describe_log_dirs,
        describe_replica_log_dirs,
        alter_replica_log_dirs,
        create_partitions,
        describe_topics,
        describe_configs,
        incremental_alter_configs,
        legacy_alter_configs,
        list_consumer_group_offsets,
        list_consumer_groups,
        delete_consumer_group_offsets,
        delete_share_group_offsets,
        list_share_group_offsets,
        alter_share_group_offsets,
        describe_share_group,
        describe_streams_group,
        alter_consumer_group_offsets,
        admin_list_offsets,
        list_partition_reassignments,
        alter_partition_reassignments,
        elect_leaders,
        remove_consumer_group_members,
    } = admin_hosts::start(admin_ports);
    let (transaction_initialization, transaction_initialization_admission, producer) =
        transaction_start::start(
            validated.host_limits,
            Arc::clone(&clock),
            &wake,
            &mut group_consumers,
            &mut admin_notifier,
            &mut assigned_consumer_notifier,
        )?;
    notifier_start::install_thread_ids(
        lifecycle,
        &producer,
        &admin_notifier,
        &assigned_consumer_notifier,
        &group_consumers,
        &share_consumers,
        &transaction_initialization,
    );
    let (group_consumers, group_consumer) =
        GroupConsumerShardOwner::new(group_consumers, Arc::clone(&clock), Arc::clone(&wake));
    let share_consumers =
        ShareShardOwner::new(share_consumers, Arc::clone(&clock), Arc::clone(&wake));
    let share_consumer = share_consumers.admission_port();
    let producer = ProducerShardOwner::new(producer, Arc::clone(&wake));
    let admission = producer.admission_port();
    let abort_partition_transaction = AbortPartitionTransactionShardOwner::new(
        abort_partition_transaction,
        Arc::new(driver.reactor_wake()),
    );
    let abort_partition_transaction_admission = abort_partition_transaction.admission_port();
    let create_topics = CreateTopicsShardOwner::new(create_topics, Arc::new(driver.reactor_wake()));
    let create_topics_admission = create_topics.admission_port();
    let create_acls = CreateAclsShardOwner::new(create_acls, Arc::new(driver.reactor_wake()));
    let create_acls_admission = create_acls.admission_port();
    let create_delegation_token =
        CreateDelegationTokenShardOwner::new(create_delegation_token, Arc::clone(&wake));
    let create_delegation_token_admission = create_delegation_token.admission_port();
    let describe_delegation_tokens =
        DescribeDelegationTokensShardOwner::new(describe_delegation_tokens, Arc::clone(&wake));
    let describe_delegation_tokens_admission = describe_delegation_tokens.admission_port();
    let renew_delegation_token =
        RenewDelegationTokenShardOwner::new(renew_delegation_token, Arc::clone(&wake));
    let renew_delegation_token_admission = renew_delegation_token.admission_port();
    let expire_delegation_token =
        ExpireDelegationTokenShardOwner::new(expire_delegation_token, Arc::clone(&wake));
    let expire_delegation_token_admission = expire_delegation_token.admission_port();
    let delete_acls = DeleteAclsShardOwner::new(delete_acls, Arc::new(driver.reactor_wake()));
    let delete_acls_admission = delete_acls.admission_port();
    let delete_topics = DeleteTopicsShardOwner::new(delete_topics, Arc::new(driver.reactor_wake()));
    let delete_topics_admission = delete_topics.admission_port();
    let delete_consumer_groups = DeleteConsumerGroupsShardOwner::new(
        delete_consumer_groups,
        Arc::new(driver.reactor_wake()),
    );
    let delete_consumer_groups_admission = delete_consumer_groups.admission_port();
    let delete_records =
        DeleteRecordsShardOwner::new(delete_records, Arc::new(driver.reactor_wake()));
    let delete_records_admission = delete_records.admission_port();
    let describe_acls = DescribeAclsShardOwner::new(describe_acls, Arc::new(driver.reactor_wake()));
    let describe_acls_admission = describe_acls.admission_port();
    let describe_client_quotas = DescribeClientQuotasShardOwner::new(
        describe_client_quotas,
        Arc::new(driver.reactor_wake()),
    );
    let describe_client_quotas_admission = describe_client_quotas.admission_port();
    let alter_client_quotas =
        AlterClientQuotasShardOwner::new(alter_client_quotas, Arc::new(driver.reactor_wake()));
    let alter_client_quotas_admission = alter_client_quotas.admission_port();
    let alter_user_scram_credentials = AlterUserScramCredentialsShardOwner::new(
        alter_user_scram_credentials,
        Arc::new(driver.reactor_wake()),
    );
    let alter_user_scram_credentials_admission = alter_user_scram_credentials.admission_port();
    let update_features =
        UpdateFeaturesShardOwner::new(update_features, Arc::new(driver.reactor_wake()));
    let update_features_admission = update_features.admission_port();
    let unregister_broker =
        UnregisterBrokerShardOwner::new(unregister_broker, Arc::new(driver.reactor_wake()));
    let unregister_broker_admission = unregister_broker.admission_port();
    let add_raft_voter =
        AddRaftVoterShardOwner::new(add_raft_voter, Arc::new(driver.reactor_wake()));
    let add_raft_voter_admission = add_raft_voter.admission_port();
    let remove_raft_voter =
        RemoveRaftVoterShardOwner::new(remove_raft_voter, Arc::new(driver.reactor_wake()));
    let remove_raft_voter_admission = remove_raft_voter.admission_port();
    let describe_user_scram_credentials = DescribeUserScramCredentialsShardOwner::new(
        describe_user_scram_credentials,
        Arc::new(driver.reactor_wake()),
    );
    let describe_user_scram_credentials_admission =
        describe_user_scram_credentials.admission_port();
    let describe_metadata_quorum = DescribeMetadataQuorumShardOwner::new(
        describe_metadata_quorum,
        Arc::new(driver.reactor_wake()),
    );
    let describe_metadata_quorum_admission = describe_metadata_quorum.admission_port();
    let describe_producers =
        AdminDescribeProducersShardOwner::new(describe_producers, Arc::new(driver.reactor_wake()));
    let describe_producers_admission = describe_producers.admission_port();
    let describe_topic_partitions = AdminDescribeTopicPartitionsShardOwner::new(
        describe_topic_partitions,
        Arc::new(driver.reactor_wake()),
    );
    let describe_topic_partitions_admission = describe_topic_partitions.admission_port();
    let describe_transactions = AdminDescribeTransactionsShardOwner::new(
        describe_transactions,
        Arc::new(driver.reactor_wake()),
    );
    let describe_transactions_admission = describe_transactions.admission_port();
    let fence_producers =
        AdminFenceProducersShardOwner::new(fence_producers, Arc::new(driver.reactor_wake()));
    let fence_producers_admission = fence_producers.admission_port();
    let list_transactions =
        AdminListTransactionsShardOwner::new(list_transactions, Arc::new(driver.reactor_wake()));
    let list_transactions_admission = list_transactions.admission_port();
    let list_client_metrics_resources = ListClientMetricsResourcesShardOwner::new(
        list_client_metrics_resources,
        Arc::new(driver.reactor_wake()),
    );
    let list_client_metrics_resources_admission = list_client_metrics_resources.admission_port();
    let list_config_resources =
        ListConfigResourcesShardOwner::new(list_config_resources, Arc::new(driver.reactor_wake()));
    let list_config_resources_admission = list_config_resources.admission_port();
    let describe_cluster =
        DescribeClusterShardOwner::new(describe_cluster, Arc::new(driver.reactor_wake()));
    let describe_cluster_admission = describe_cluster.admission_port();
    let describe_consumer_groups = DescribeConsumerGroupsShardOwner::new(
        describe_consumer_groups,
        Arc::new(driver.reactor_wake()),
    );
    let describe_consumer_groups_admission = describe_consumer_groups.admission_port();
    let describe_features =
        DescribeFeaturesShardOwner::new(describe_features, Arc::new(driver.reactor_wake()));
    let describe_features_admission = describe_features.admission_port();
    let describe_log_dirs =
        DescribeLogDirsShardOwner::new(describe_log_dirs, Arc::new(driver.reactor_wake()));
    let describe_log_dirs_admission = describe_log_dirs.admission_port();
    let describe_replica_log_dirs = DescribeReplicaLogDirsShardOwner::new(
        describe_replica_log_dirs,
        Arc::new(driver.reactor_wake()),
    );
    let describe_replica_log_dirs_admission = describe_replica_log_dirs.admission_port();
    let alter_replica_log_dirs =
        AlterReplicaLogDirsShardOwner::new(alter_replica_log_dirs, Arc::new(driver.reactor_wake()));
    let alter_replica_log_dirs_admission = alter_replica_log_dirs.admission_port();
    let create_partitions =
        CreatePartitionsShardOwner::new(create_partitions, Arc::new(driver.reactor_wake()));
    let create_partitions_admission = create_partitions.admission_port();
    let describe_topics =
        DescribeTopicsShardOwner::new(describe_topics, Arc::new(driver.reactor_wake()));
    let describe_topics_admission = describe_topics.admission_port();
    let config_admin = describe_configs_start::start(
        describe_configs,
        incremental_alter_configs,
        legacy_alter_configs,
        Arc::new(driver.reactor_wake()),
    );
    let list_consumer_group_offsets =
        ListConsumerGroupOffsetsShardOwner::new(list_consumer_group_offsets, Arc::clone(&wake));
    let list_consumer_group_offsets_admission = list_consumer_group_offsets.admission_port();
    let list_consumer_groups =
        ListConsumerGroupsShardOwner::new(list_consumer_groups, Arc::clone(&wake));
    let list_consumer_groups_admission = list_consumer_groups.admission_port();
    let delete_consumer_group_offsets =
        DeleteConsumerGroupOffsetsShardOwner::new(delete_consumer_group_offsets, Arc::clone(&wake));
    let delete_consumer_group_offsets_admission = delete_consumer_group_offsets.admission_port();
    let share_group_offsets = share_group_offsets::start(
        delete_share_group_offsets,
        list_share_group_offsets,
        alter_share_group_offsets,
        describe_share_group,
        describe_streams_group,
        Arc::clone(&wake),
    );
    let alter_consumer_group_offsets = alter_consumer_group_offsets_start::start(
        alter_consumer_group_offsets,
        driver.reactor_wake(),
    );
    let list_offsets = list_offsets::start(admin_list_offsets, driver.reactor_wake());
    let list_partition_reassignments =
        list_partition_reassignments::start(list_partition_reassignments, driver.reactor_wake());
    let alter_partition_reassignments =
        alter_partition_reassignments::start(alter_partition_reassignments, driver.reactor_wake());
    let elect_leaders = ElectLeadersShardOwner::new(elect_leaders, Arc::new(driver.reactor_wake()));
    let elect_leaders_admission = elect_leaders.admission_port();
    let remove_consumer_group_members = RemoveConsumerGroupMembersShardOwner::new(
        remove_consumer_group_members,
        Arc::new(driver.reactor_wake()),
    );
    let remove_consumer_group_members_admission = remove_consumer_group_members.admission_port();
    let produce_calls = crate::driver::TrackedProduceCalls::with_max_in_flight_requests_per_broker(
        validated.host_limits.batch_capacity,
        validated.host_limits.max_in_flight_requests_per_broker,
    );
    let resources = EngineHostResources {
        driver: Some(driver),
        producer,
        admin_notifier,
        assigned_consumer_notifier,
        abort_partition_transaction,
        add_raft_voter,
        remove_raft_voter,
        create_topics,
        create_acls,
        create_delegation_token,
        describe_delegation_tokens,
        renew_delegation_token,
        expire_delegation_token,
        delete_acls,
        delete_topics,
        delete_consumer_groups,
        delete_records,
        describe_acls,
        describe_client_quotas,
        alter_client_quotas,
        alter_user_scram_credentials,
        update_features,
        unregister_broker,
        describe_user_scram_credentials,
        describe_metadata_quorum,
        describe_producers,
        describe_topic_partitions,
        describe_transactions,
        fence_producers,
        list_transactions,
        list_client_metrics_resources,
        list_config_resources,
        describe_cluster,
        describe_consumer_groups,
        describe_features,
        describe_log_dirs,
        describe_replica_log_dirs,
        alter_replica_log_dirs,
        create_partitions,
        describe_topics,
        describe_configs: config_admin.describe_owner,
        incremental_alter_configs: config_admin.incremental_owner,
        legacy_alter_configs: config_admin.legacy_owner,
        list_consumer_group_offsets,
        list_consumer_groups,
        delete_consumer_group_offsets,
        delete_share_group_offsets: share_group_offsets.delete,
        list_share_group_offsets: share_group_offsets.list,
        alter_share_group_offsets: share_group_offsets.alter,
        describe_share_group: share_group_offsets.describe,
        describe_streams_group: share_group_offsets.describe_streams,
        alter_consumer_group_offsets: alter_consumer_group_offsets.owner,
        list_offsets: list_offsets.owner,
        list_partition_reassignments: list_partition_reassignments.owner,
        alter_partition_reassignments: alter_partition_reassignments.owner,
        elect_leaders,
        remove_consumer_group_members,
        assigned_consumer: assigned_consumer_owner,
        group_consumers,
        share_consumers,
        transaction_initialization,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
        produce_calls,
        producer_identity_calls: crate::driver::TrackedProducerIdentityCalls::new(),
        producer_partitioning_call: None,
        producer_retry_identity_call: None,
        create_topics_calls: crate::driver::TrackedCreateTopicsCalls::new(
            crate::admin::CREATE_TOPICS_CAPACITY,
        ),
        delete_topics_calls: crate::driver::TrackedDeleteTopicsCalls::new(
            crate::admin::DELETE_TOPICS_CAPACITY,
        ),
        describe_cluster_calls: crate::driver::DescribeClusterCalls::new(
            crate::admin::DESCRIBE_CLUSTER_CAPACITY,
        ),
        create_partitions_calls: crate::driver::TrackedCreatePartitionsCalls::new(
            crate::admin::CREATE_PARTITIONS_CAPACITY,
        ),
        describe_topics_calls: crate::driver::DescribeTopicsCalls::new(
            crate::admin::DESCRIBE_TOPICS_CAPACITY,
        ),
        describe_configs_calls: config_admin.describe_calls,
        incremental_alter_configs_calls: config_admin.incremental_calls,
    };
    let started = StartedEngineHost {
        metrics,
        admission,
        abort_partition_transaction_admission,
        add_raft_voter_admission,
        remove_raft_voter_admission,
        create_topics_admission,
        create_acls_admission,
        create_delegation_token_admission,
        describe_delegation_tokens_admission,
        renew_delegation_token_admission,
        expire_delegation_token_admission,
        delete_acls_admission,
        delete_topics_admission,
        delete_consumer_groups_admission,
        delete_records_admission,
        describe_acls_admission,
        describe_client_quotas_admission,
        alter_client_quotas_admission,
        alter_user_scram_credentials_admission,
        update_features_admission,
        unregister_broker_admission,
        describe_user_scram_credentials_admission,
        describe_metadata_quorum_admission,
        describe_producers_admission,
        describe_topic_partitions_admission,
        describe_transactions_admission,
        fence_producers_admission,
        list_transactions_admission,
        list_client_metrics_resources_admission,
        list_config_resources_admission,
        describe_cluster_admission,
        describe_consumer_groups_admission,
        describe_features_admission,
        describe_log_dirs_admission,
        describe_replica_log_dirs_admission,
        alter_replica_log_dirs_admission,
        create_partitions_admission,
        describe_topics_admission,
        describe_configs_admission: config_admin.describe_admission,
        incremental_alter_configs_admission: config_admin.incremental_admission,
        legacy_alter_configs_admission: config_admin.legacy_admission,
        list_consumer_group_offsets_admission,
        list_consumer_groups_admission,
        delete_consumer_group_offsets_admission,
        delete_share_group_offsets_admission: share_group_offsets.delete_admission,
        list_share_group_offsets_admission: share_group_offsets.list_admission,
        alter_share_group_offsets_admission: share_group_offsets.alter_admission,
        describe_share_group_admission: share_group_offsets.describe_admission,
        describe_streams_group_admission: share_group_offsets.describe_streams_admission,
        alter_consumer_group_offsets_admission: alter_consumer_group_offsets.admission,
        list_offsets_admission: list_offsets.admission,
        list_partition_reassignments_admission: list_partition_reassignments.admission,
        alter_partition_reassignments_admission: alter_partition_reassignments.admission,
        elect_leaders_admission,
        remove_consumer_group_members_admission,
        assigned_consumer,
        group_consumer,
        share_consumer,
        transaction_initialization: transaction_initialization_admission,
        clock,
        control,
        lifecycle: Arc::clone(lifecycle),
    };
    Ok((resources, started))
}
