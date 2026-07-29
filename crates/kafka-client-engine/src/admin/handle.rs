//! Runtime-neutral public handle retaining concrete admin admission ports.

use std::{fmt, sync::Arc, time::Duration};

use super::{
    CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind, CreateTopicsObserver,
    CreateTopicsRequest, DeleteTopicsAdmissionPort,
    list_client_metrics_resources::internal_api::ListClientMetricsResourcesAdmissionPort,
    list_config_resources::ListConfigResourcesAdmissionPort, shard::CreateTopicsAdmissionPort,
};
use crate::clock::MonotonicClock;

/// Closed set of concrete admin admission capabilities retained by one handle.
pub(crate) struct AdminAdmissionPorts {
    pub(crate) abort_partition_transaction:
        super::abort_partition_transaction::AbortPartitionTransactionAdmissionPort,
    pub(crate) add_raft_voter: super::add_raft_voter::AddRaftVoterAdmissionPort,
    pub(crate) remove_raft_voter: super::remove_raft_voter::RemoveRaftVoterAdmissionPort,
    pub(crate) create_topics: CreateTopicsAdmissionPort,
    pub(crate) create_acls: super::CreateAclsAdmissionPort,
    pub(crate) create_delegation_token: super::CreateDelegationTokenAdmissionPort,
    pub(crate) describe_delegation_tokens: super::DescribeDelegationTokensAdmissionPort,
    pub(crate) renew_delegation_token: super::RenewDelegationTokenAdmissionPort,
    pub(crate) expire_delegation_token: super::ExpireDelegationTokenAdmissionPort,
    pub(crate) delete_acls: super::DeleteAclsAdmissionPort,
    pub(crate) delete_topics: DeleteTopicsAdmissionPort,
    pub(crate) delete_records: super::DeleteRecordsAdmissionPort,
    pub(crate) delete_share_group_offsets:
        super::delete_share_group_offsets::DeleteShareGroupOffsetsAdmissionPort,
    pub(crate) list_share_group_offsets:
        super::list_share_group_offsets::ListShareGroupOffsetsAdmissionPort,
    pub(crate) alter_share_group_offsets:
        super::alter_share_group_offsets::AlterShareGroupOffsetsAdmissionPort,
    pub(crate) describe_share_group: super::describe_share_group::DescribeShareGroupAdmissionPort,
    pub(crate) describe_streams_group:
        super::describe_streams_group::DescribeStreamsGroupAdmissionPort,
    pub(crate) describe_acls: super::DescribeAclsAdmissionPort,
    pub(crate) describe_client_quotas: super::DescribeClientQuotasAdmissionPort,
    pub(crate) alter_client_quotas: super::AlterClientQuotasAdmissionPort,
    pub(crate) alter_user_scram_credentials: super::AlterUserScramCredentialsAdmissionPort,
    pub(crate) update_features: super::update_features::UpdateFeaturesAdmissionPort,
    pub(crate) unregister_broker: super::unregister_broker::UnregisterBrokerAdmissionPort,
    pub(crate) describe_user_scram_credentials: super::DescribeUserScramCredentialsAdmissionPort,
    pub(crate) describe_metadata_quorum: super::DescribeMetadataQuorumAdmissionPort,
    pub(crate) describe_producers: super::AdminDescribeProducersAdmissionPort,
    pub(crate) describe_topic_partitions: super::AdminDescribeTopicPartitionsAdmissionPort,
    pub(crate) describe_transactions: super::AdminDescribeTransactionsAdmissionPort,
    pub(crate) fence_producers: super::AdminFenceProducersAdmissionPort,
    pub(crate) list_transactions: super::AdminListTransactionsAdmissionPort,
    pub(crate) list_client_metrics_resources: ListClientMetricsResourcesAdmissionPort,
    pub(crate) list_config_resources: ListConfigResourcesAdmissionPort,
    pub(crate) describe_cluster: super::DescribeClusterAdmissionPort,
    pub(crate) describe_consumer_groups: super::DescribeConsumerGroupsAdmissionPort,
    pub(crate) describe_features: super::describe_features::DescribeFeaturesAdmissionPort,
    pub(crate) describe_log_dirs: super::DescribeLogDirsAdmissionPort,
    pub(crate) describe_replica_log_dirs: super::DescribeReplicaLogDirsAdmissionPort,
    pub(crate) create_partitions: super::CreatePartitionsAdmissionPort,
    pub(crate) describe_topics: super::DescribeTopicsAdmissionPort,
    pub(crate) describe_configs: super::DescribeConfigsAdmissionPort,
    pub(crate) incremental_alter_configs: super::IncrementalAlterConfigsAdmissionPort,
    pub(crate) legacy_alter_configs: super::LegacyAlterConfigsAdmissionPort,
    pub(crate) list_consumer_group_offsets: super::ListConsumerGroupOffsetsAdmissionPort,
    pub(crate) list_consumer_groups: super::ListConsumerGroupsAdmissionPort,
    pub(crate) delete_consumer_group_offsets: super::DeleteConsumerGroupOffsetsAdmissionPort,
    pub(crate) delete_consumer_groups: super::DeleteConsumerGroupsAdmissionPort,
    pub(crate) alter_consumer_group_offsets: super::AlterConsumerGroupOffsetsAdmissionPort,
    pub(crate) list_offsets: super::AdminListOffsetsAdmissionPort,
    pub(crate) list_partition_reassignments: super::ListPartitionReassignmentsAdmissionPort,
    pub(crate) alter_partition_reassignments: super::AlterPartitionReassignmentsAdmissionPort,
    pub(crate) alter_replica_log_dirs: super::AlterReplicaLogDirsAdmissionPort,
    pub(crate) elect_leaders: super::ElectLeadersAdmissionPort,
    pub(crate) remove_consumer_group_members: super::RemoveConsumerGroupMembersAdmissionPort,
}

/// Cheaply cloneable handle to the concrete admin shards.
#[derive(Clone)]
pub struct AdminHandle {
    pub(super) abort_partition_transaction:
        super::abort_partition_transaction::AbortPartitionTransactionAdmissionPort,
    pub(super) add_raft_voter: super::add_raft_voter::AddRaftVoterAdmissionPort,
    pub(super) remove_raft_voter: super::remove_raft_voter::RemoveRaftVoterAdmissionPort,
    pub(super) create_topics: CreateTopicsAdmissionPort,
    pub(super) create_acls: super::CreateAclsAdmissionPort,
    pub(super) create_delegation_token: super::CreateDelegationTokenAdmissionPort,
    pub(super) describe_delegation_tokens: super::DescribeDelegationTokensAdmissionPort,
    pub(super) renew_delegation_token: super::RenewDelegationTokenAdmissionPort,
    pub(super) expire_delegation_token: super::ExpireDelegationTokenAdmissionPort,
    pub(super) delete_acls: super::DeleteAclsAdmissionPort,
    pub(super) delete_topics: DeleteTopicsAdmissionPort,
    pub(super) delete_records: super::DeleteRecordsAdmissionPort,
    pub(super) delete_share_group_offsets:
        super::delete_share_group_offsets::DeleteShareGroupOffsetsAdmissionPort,
    pub(super) list_share_group_offsets:
        super::list_share_group_offsets::ListShareGroupOffsetsAdmissionPort,
    pub(super) alter_share_group_offsets:
        super::alter_share_group_offsets::AlterShareGroupOffsetsAdmissionPort,
    pub(super) describe_share_group: super::describe_share_group::DescribeShareGroupAdmissionPort,
    pub(super) describe_streams_group:
        super::describe_streams_group::DescribeStreamsGroupAdmissionPort,
    pub(super) describe_acls: super::DescribeAclsAdmissionPort,
    pub(super) describe_client_quotas: super::DescribeClientQuotasAdmissionPort,
    pub(super) alter_client_quotas: super::AlterClientQuotasAdmissionPort,
    pub(super) alter_user_scram_credentials: super::AlterUserScramCredentialsAdmissionPort,
    pub(super) update_features: super::update_features::UpdateFeaturesAdmissionPort,
    pub(super) unregister_broker: super::unregister_broker::UnregisterBrokerAdmissionPort,
    pub(super) describe_user_scram_credentials: super::DescribeUserScramCredentialsAdmissionPort,
    pub(super) describe_metadata_quorum: super::DescribeMetadataQuorumAdmissionPort,
    pub(super) describe_producers: super::AdminDescribeProducersAdmissionPort,
    pub(super) describe_topic_partitions: super::AdminDescribeTopicPartitionsAdmissionPort,
    pub(super) describe_transactions: super::AdminDescribeTransactionsAdmissionPort,
    pub(super) fence_producers: super::AdminFenceProducersAdmissionPort,
    pub(super) list_transactions: super::AdminListTransactionsAdmissionPort,
    pub(super) list_client_metrics_resources: ListClientMetricsResourcesAdmissionPort,
    pub(super) list_config_resources: ListConfigResourcesAdmissionPort,
    pub(super) describe_cluster: super::DescribeClusterAdmissionPort,
    pub(super) describe_consumer_groups: super::DescribeConsumerGroupsAdmissionPort,
    pub(super) describe_features: super::describe_features::DescribeFeaturesAdmissionPort,
    pub(super) describe_log_dirs: super::DescribeLogDirsAdmissionPort,
    pub(super) describe_replica_log_dirs: super::DescribeReplicaLogDirsAdmissionPort,
    pub(super) create_partitions: super::CreatePartitionsAdmissionPort,
    pub(super) describe_topics: super::DescribeTopicsAdmissionPort,
    pub(super) describe_configs: super::DescribeConfigsAdmissionPort,
    pub(super) incremental_alter_configs: super::IncrementalAlterConfigsAdmissionPort,
    pub(super) legacy_alter_configs: super::LegacyAlterConfigsAdmissionPort,
    pub(super) list_consumer_group_offsets: super::ListConsumerGroupOffsetsAdmissionPort,
    pub(super) list_consumer_groups: super::ListConsumerGroupsAdmissionPort,
    pub(super) delete_consumer_group_offsets: super::DeleteConsumerGroupOffsetsAdmissionPort,
    pub(super) delete_consumer_groups: super::DeleteConsumerGroupsAdmissionPort,
    pub(super) alter_consumer_group_offsets: super::AlterConsumerGroupOffsetsAdmissionPort,
    pub(super) list_offsets: super::AdminListOffsetsAdmissionPort,
    pub(super) list_partition_reassignments: super::ListPartitionReassignmentsAdmissionPort,
    pub(super) alter_partition_reassignments: super::AlterPartitionReassignmentsAdmissionPort,
    pub(super) alter_replica_log_dirs: super::AlterReplicaLogDirsAdmissionPort,
    pub(super) elect_leaders: super::ElectLeadersAdmissionPort,
    pub(super) remove_consumer_group_members: super::RemoveConsumerGroupMembersAdmissionPort,
    pub(super) clock: Arc<MonotonicClock>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl AdminHandle {
    pub(crate) fn new(
        ports: AdminAdmissionPorts,
        clock: Arc<MonotonicClock>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            abort_partition_transaction: ports.abort_partition_transaction,
            add_raft_voter: ports.add_raft_voter,
            remove_raft_voter: ports.remove_raft_voter,
            create_topics: ports.create_topics,
            create_acls: ports.create_acls,
            create_delegation_token: ports.create_delegation_token,
            describe_delegation_tokens: ports.describe_delegation_tokens,
            renew_delegation_token: ports.renew_delegation_token,
            expire_delegation_token: ports.expire_delegation_token,
            delete_acls: ports.delete_acls,
            delete_topics: ports.delete_topics,
            delete_records: ports.delete_records,
            delete_share_group_offsets: ports.delete_share_group_offsets,
            list_share_group_offsets: ports.list_share_group_offsets,
            alter_share_group_offsets: ports.alter_share_group_offsets,
            describe_share_group: ports.describe_share_group,
            describe_streams_group: ports.describe_streams_group,
            describe_acls: ports.describe_acls,
            describe_client_quotas: ports.describe_client_quotas,
            alter_client_quotas: ports.alter_client_quotas,
            alter_user_scram_credentials: ports.alter_user_scram_credentials,
            update_features: ports.update_features,
            unregister_broker: ports.unregister_broker,
            describe_user_scram_credentials: ports.describe_user_scram_credentials,
            describe_metadata_quorum: ports.describe_metadata_quorum,
            describe_producers: ports.describe_producers,
            describe_topic_partitions: ports.describe_topic_partitions,
            describe_transactions: ports.describe_transactions,
            fence_producers: ports.fence_producers,
            list_transactions: ports.list_transactions,
            list_client_metrics_resources: ports.list_client_metrics_resources,
            list_config_resources: ports.list_config_resources,
            describe_cluster: ports.describe_cluster,
            describe_consumer_groups: ports.describe_consumer_groups,
            describe_features: ports.describe_features,
            describe_log_dirs: ports.describe_log_dirs,
            describe_replica_log_dirs: ports.describe_replica_log_dirs,
            create_partitions: ports.create_partitions,
            describe_topics: ports.describe_topics,
            describe_configs: ports.describe_configs,
            incremental_alter_configs: ports.incremental_alter_configs,
            legacy_alter_configs: ports.legacy_alter_configs,
            list_consumer_group_offsets: ports.list_consumer_group_offsets,
            list_consumer_groups: ports.list_consumer_groups,
            delete_consumer_group_offsets: ports.delete_consumer_group_offsets,
            delete_consumer_groups: ports.delete_consumer_groups,
            alter_consumer_group_offsets: ports.alter_consumer_group_offsets,
            list_offsets: ports.list_offsets,
            list_partition_reassignments: ports.list_partition_reassignments,
            alter_partition_reassignments: ports.alter_partition_reassignments,
            alter_replica_log_dirs: ports.alter_replica_log_dirs,
            elect_leaders: ports.elect_leaders,
            remove_consumer_group_members: ports.remove_consumer_group_members,
            clock,
            _lifetime: lifetime,
        }
    }

    /// Attempts immediate bounded admission using one call-boundary deadline.
    pub fn try_create_topics(
        &self,
        request: CreateTopicsRequest,
        timeout: Duration,
    ) -> Result<CreateTopicsAccepted, CreateTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(CreateTopicsAdmissionError::new(
                CreateTopicsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .create_topics
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(CreateTopicsAdmissionError::new)?;
        Ok(CreateTopicsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for AdminHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminHandle")
            .finish_non_exhaustive()
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::CreateTopicsHostError,
) -> CreateTopicsAcceptedFaultKind {
    match fault {
        super::CreateTopicsHostError::Wake => CreateTopicsAcceptedFaultKind::Wake,
        _ => CreateTopicsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke operation ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted CreateTopics work must retain its observer"]
pub struct CreateTopicsAccepted {
    observer: CreateTopicsObserver,
    fault: Option<CreateTopicsAcceptedFaultKind>,
}

impl CreateTopicsAccepted {
    /// Returns any post-commit degradation without misclassifying ownership.
    pub const fn fault(&self) -> Option<CreateTopicsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> CreateTopicsObserver {
        self.observer
    }
}

impl fmt::Debug for CreateTopicsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateTopicsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
