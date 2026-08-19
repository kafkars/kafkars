//! Shared public owner of one reactor-native execution host.

mod admin;

use std::sync::Arc;

use crate::{
    AssignedConsumerClaimError, AssignedConsumerHandle, EngineConfig, ProducerHandle,
    engine_host::{
        EngineHostControl, EngineLifecycle, EngineShutdownError, EngineStartError,
        StartedEngineHost, start as start_host,
    },
};

/// Cheaply cloneable owner of one embedded driver and shared execution host.
#[derive(Clone)]
pub struct Engine {
    pub(crate) inner: Arc<EngineInner>,
}

pub(crate) struct EngineInner {
    pub(crate) config: EngineConfig,
    pub(crate) metrics: crate::driver::owner::observation::DriverObservationHandle,
    pub(crate) admission: crate::producer::ingress::ProducerAdmissionPort,
    abort_partition_transaction_admission:
        crate::admin::AbortPartitionTransactionAdmissionPort,
    create_topics_admission: crate::admin::CreateTopicsAdmissionPort,
    create_acls_admission: crate::admin::CreateAclsAdmissionPort,
    create_delegation_token_admission: crate::admin::CreateDelegationTokenAdmissionPort,
    describe_delegation_tokens_admission: crate::admin::DescribeDelegationTokensAdmissionPort,
    renew_delegation_token_admission: crate::admin::RenewDelegationTokenAdmissionPort,
    expire_delegation_token_admission: crate::admin::ExpireDelegationTokenAdmissionPort,
    delete_acls_admission: crate::admin::DeleteAclsAdmissionPort,
    delete_topics_admission: crate::admin::DeleteTopicsAdmissionPort,
    delete_consumer_groups_admission: crate::admin::DeleteConsumerGroupsAdmissionPort,
    delete_records_admission: crate::admin::DeleteRecordsAdmissionPort,
    describe_acls_admission: crate::admin::DescribeAclsAdmissionPort,
    describe_client_quotas_admission: crate::admin::DescribeClientQuotasAdmissionPort,
    alter_client_quotas_admission: crate::admin::AlterClientQuotasAdmissionPort,
    alter_user_scram_credentials_admission: crate::admin::AlterUserScramCredentialsAdmissionPort,
    update_features_admission: crate::admin::update_features::UpdateFeaturesAdmissionPort,
    unregister_broker_admission: crate::admin::unregister_broker::UnregisterBrokerAdmissionPort,
    add_raft_voter_admission: crate::admin::AddRaftVoterAdmissionPort,
    remove_raft_voter_admission: crate::admin::RemoveRaftVoterAdmissionPort,
    describe_user_scram_credentials_admission:
        crate::admin::DescribeUserScramCredentialsAdmissionPort,
    describe_metadata_quorum_admission: crate::admin::DescribeMetadataQuorumAdmissionPort,
    describe_producers_admission: crate::admin::AdminDescribeProducersAdmissionPort,
    describe_topic_partitions_admission: crate::admin::AdminDescribeTopicPartitionsAdmissionPort,
    describe_transactions_admission: crate::admin::AdminDescribeTransactionsAdmissionPort,
    fence_producers_admission: crate::admin::AdminFenceProducersAdmissionPort,
    list_transactions_admission: crate::admin::AdminListTransactionsAdmissionPort,
    list_client_metrics_resources_admission:
        crate::admin::list_client_metrics_resources::internal_api::
            ListClientMetricsResourcesAdmissionPort,
    list_config_resources_admission:
        crate::admin::list_config_resources::ListConfigResourcesAdmissionPort,
    describe_cluster_admission: crate::admin::DescribeClusterAdmissionPort,
    describe_consumer_groups_admission: crate::admin::DescribeConsumerGroupsAdmissionPort,
    describe_features_admission: crate::admin::DescribeFeaturesAdmissionPort,
    describe_log_dirs_admission: crate::admin::DescribeLogDirsAdmissionPort,
    describe_replica_log_dirs_admission: crate::admin::DescribeReplicaLogDirsAdmissionPort,
    create_partitions_admission: crate::admin::CreatePartitionsAdmissionPort,
    describe_topics_admission: crate::admin::DescribeTopicsAdmissionPort,
    describe_configs_admission: crate::admin::DescribeConfigsAdmissionPort,
    incremental_alter_configs_admission: crate::admin::IncrementalAlterConfigsAdmissionPort,
    legacy_alter_configs_admission: crate::admin::LegacyAlterConfigsAdmissionPort,
    list_consumer_group_offsets_admission: crate::admin::ListConsumerGroupOffsetsAdmissionPort,
    list_consumer_groups_admission: crate::admin::ListConsumerGroupsAdmissionPort,
    delete_consumer_group_offsets_admission: crate::admin::DeleteConsumerGroupOffsetsAdmissionPort,
    delete_share_group_offsets_admission:
        crate::admin::delete_share_group_offsets::DeleteShareGroupOffsetsAdmissionPort,
    list_share_group_offsets_admission:
        crate::admin::list_share_group_offsets::ListShareGroupOffsetsAdmissionPort,
    alter_share_group_offsets_admission:
        crate::admin::alter_share_group_offsets::AlterShareGroupOffsetsAdmissionPort,
    describe_share_group_admission:
        crate::admin::describe_share_group::DescribeShareGroupAdmissionPort,
    describe_streams_group_admission:
        crate::admin::describe_streams_group::DescribeStreamsGroupAdmissionPort,
    alter_consumer_group_offsets_admission: crate::admin::AlterConsumerGroupOffsetsAdmissionPort,
    list_offsets_admission: crate::admin::AdminListOffsetsAdmissionPort,
    list_partition_reassignments_admission: crate::admin::ListPartitionReassignmentsAdmissionPort,
    alter_partition_reassignments_admission: crate::admin::AlterPartitionReassignmentsAdmissionPort,
    alter_replica_log_dirs_admission: crate::admin::AlterReplicaLogDirsAdmissionPort,
    elect_leaders_admission: crate::admin::ElectLeadersAdmissionPort,
    remove_consumer_group_members_admission: crate::admin::RemoveConsumerGroupMembersAdmissionPort,
    assigned_consumer: crate::consumer::AssignedConsumerClaimSlot,
    assigned_consumer_admission: crate::consumer::AssignedConsumerAdmissionCloser,
    pub(crate) group_consumer: crate::consumer::GroupConsumerPort,
    pub(crate) transaction_initialization:
        crate::transaction::TransactionInitializationAdmissionPort,
    clock: Arc<crate::clock::MonotonicClock>,
    control: Arc<EngineHostControl>,
    lifecycle: Arc<EngineLifecycle>,
}

impl Engine {
    /// Validates every local bound and starts one native host thread.
    #[allow(
        clippy::too_many_lines,
        reason = "the one-to-one transfer of every admission capability into the shared owner stays explicit and auditable"
    )]
    pub fn start(config: EngineConfig) -> Result<Self, EngineStartError> {
        let validated = config.validate().map_err(EngineStartError::configuration)?;
        let StartedEngineHost {
            metrics,
            admission,
            abort_partition_transaction_admission,
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
            add_raft_voter_admission,
            remove_raft_voter_admission,
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
            create_partitions_admission,
            describe_topics_admission,
            describe_configs_admission,
            incremental_alter_configs_admission,
            legacy_alter_configs_admission,
            list_consumer_group_offsets_admission,
            list_consumer_groups_admission,
            delete_consumer_group_offsets_admission,
            delete_share_group_offsets_admission,
            list_share_group_offsets_admission,
            alter_share_group_offsets_admission,
            describe_share_group_admission,
            describe_streams_group_admission,
            alter_consumer_group_offsets_admission,
            list_offsets_admission,
            list_partition_reassignments_admission,
            alter_partition_reassignments_admission,
            alter_replica_log_dirs_admission,
            elect_leaders_admission,
            remove_consumer_group_members_admission,
            assigned_consumer,
            group_consumer,
            transaction_initialization,
            clock,
            control,
            lifecycle,
        } = start_host(&config, validated)?;
        let (assigned_consumer, assigned_consumer_admission) =
            crate::consumer::AssignedConsumerClaimSlot::create_for_engine(assigned_consumer);
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                metrics,
                admission,
                abort_partition_transaction_admission,
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
                add_raft_voter_admission,
                remove_raft_voter_admission,
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
                create_partitions_admission,
                describe_topics_admission,
                describe_configs_admission,
                incremental_alter_configs_admission,
                legacy_alter_configs_admission,
                list_consumer_group_offsets_admission,
                list_consumer_groups_admission,
                delete_consumer_group_offsets_admission,
                delete_share_group_offsets_admission,
                list_share_group_offsets_admission,
                alter_share_group_offsets_admission,
                describe_share_group_admission,
                describe_streams_group_admission,
                alter_consumer_group_offsets_admission,
                list_offsets_admission,
                list_partition_reassignments_admission,
                alter_partition_reassignments_admission,
                alter_replica_log_dirs_admission,
                elect_leaders_admission,
                remove_consumer_group_members_admission,
                assigned_consumer,
                assigned_consumer_admission,
                group_consumer,
                transaction_initialization,
                clock,
                control,
                lifecycle,
            }),
        })
    }
    /// Returns a runtime-neutral producer handle retaining this host.
    pub fn producer(&self) -> ProducerHandle {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        ProducerHandle::from_port(
            self.inner.admission.clone(),
            Arc::clone(&self.inner.clock),
            self.inner.config.producer_limits().in_flight_records(),
            lifetime,
        )
    }
    /// Claims the host's sole directly assigned consumer.
    pub fn claim_assigned_consumer(
        &self,
    ) -> Result<AssignedConsumerHandle, AssignedConsumerClaimError> {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        self.inner.assigned_consumer.claim(lifetime)
    }
    /// Returns immutable engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    /// Waits for the host's retained report after native resource cleanup.
    pub fn shutdown(&self) -> Result<(), EngineShutdownError> {
        self.inner.shutdown()
    }

    /// Permanently closes admission and requests host shutdown without waiting.
    pub fn request_shutdown(&self) {
        self.inner.close_admission();
        self.inner.lifecycle.request(&self.inner.control);
    }

    #[cfg(test)]
    pub(crate) fn host_snapshot(&self) -> crate::engine_host::EngineHostSnapshot {
        self.inner.control.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn force_host_failure(&self) {
        self.inner.control.request_failure();
    }

    #[cfg(test)]
    pub(crate) fn pause_after_produce_admission(&self) {
        self.inner.control.request_pause_after_produce_admission();
    }

    #[cfg(test)]
    pub(crate) fn host_is_closed(&self) -> bool {
        self.inner.lifecycle.is_closed()
    }

    #[cfg(test)]
    pub(crate) fn host_is_closing(&self) -> bool {
        self.inner.lifecycle.is_closing()
    }

    #[cfg(test)]
    pub(crate) fn completion_notifier_thread_count(&self) -> usize {
        self.inner.lifecycle.notifier_thread_count()
    }

    #[cfg(test)]
    pub(crate) fn host_probe(&self) -> EngineHostProbe {
        EngineHostProbe {
            lifecycle: Arc::clone(&self.inner.lifecycle),
        }
    }
}

impl EngineInner {
    fn shutdown(&self) -> Result<(), EngineShutdownError> {
        self.close_admission();
        self.lifecycle.request_and_wait(&self.control)
    }

    fn close_admission(&self) {
        let _close_result = self.admission.close_admission();
        self.close_admin_admission();
        let _close_result = self.assigned_consumer_admission.close();
        self.group_consumer.close_admission();
        self.transaction_initialization.close_admission();
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        self.close_admission();
        self.lifecycle.request(&self.control);
    }
}

#[cfg(test)]
pub(crate) struct EngineHostProbe {
    lifecycle: Arc<EngineLifecycle>,
}

#[cfg(test)]
impl EngineHostProbe {
    pub(crate) fn wait_closed(&self, timeout: std::time::Duration) -> bool {
        self.lifecycle.wait_closed(timeout)
    }
}
