//! One bounded notifier worker shared only by concrete admin terminal owners.

mod publishers;

use std::thread::ThreadId;

use kafka_client_core::{
    AbortPartitionTransactionTerminal, AddRaftVoterTerminal, AdminDescribeConsumerGroupsTerminal,
    AdminDescribeLogDirsTerminal, AdminDescribeProducersTerminal,
    AdminDescribeTransactionsTerminal, AdminFenceProducersTerminal,
    AdminListConsumerGroupsTerminal, AdminListOffsetsTerminal, AdminListTransactionsTerminal,
    AlterClientQuotasTerminal, AlterConsumerGroupOffsetsTerminal,
    AlterPartitionReassignmentsTerminal, AlterReplicaLogDirsTerminal,
    AlterShareGroupOffsetsTerminal, AlterUserScramCredentialsTerminal,
    CreateDelegationTokenTerminal, CreatePartitionsTerminal, CreateTopicsTerminal,
    DeleteConsumerGroupOffsetsTerminal, DeleteConsumerGroupsTerminal, DeleteRecordsTerminal,
    DeleteShareGroupOffsetsTerminal, DeleteTopicsTerminal, DescribeAclsTerminal,
    DescribeClientQuotasTerminal, DescribeClusterTerminal, DescribeConfigsTerminal,
    DescribeDelegationTokensTerminal, DescribeFeaturesTerminal, DescribeMetadataQuorumTerminal,
    DescribeReplicaLogDirsTerminal, DescribeShareGroupTerminal, DescribeStreamsGroupTerminal,
    DescribeTopicPartitionsTerminal, DescribeTopicsTerminal, DescribeUserScramCredentialsTerminal,
    ElectLeadersTerminal, ExpireDelegationTokenTerminal, IncrementalAlterConfigsTerminal,
    LegacyAlterConfigsTerminal, ListClientMetricsResourcesTerminal, ListConfigResourcesTerminal,
    ListConsumerGroupOffsetsTerminal, ListPartitionReassignmentsTerminal,
    ListShareGroupOffsetsTerminal, RemoveConsumerGroupMembersTerminal, RemoveRaftVoterTerminal,
    RenewDelegationTokenTerminal, UnregisterBrokerTerminal, UpdateFeaturesTerminal,
};

use super::{
    ABORT_PARTITION_TRANSACTION_CAPACITY, ADD_RAFT_VOTER_CAPACITY,
    CREATE_DELEGATION_TOKEN_CAPACITY, CreateAclsOutcome, DESCRIBE_DELEGATION_TOKENS_CAPACITY,
    DeleteAclsOutcome, EXPIRE_DELEGATION_TOKEN_CAPACITY, REMOVE_RAFT_VOTER_CAPACITY,
    RENEW_DELEGATION_TOKEN_CAPACITY, alter_share_group_offsets::ALTER_SHARE_GROUP_OFFSETS_CAPACITY,
    delete_share_group_offsets::DELETE_SHARE_GROUP_OFFSETS_CAPACITY,
    describe_share_group::DESCRIBE_SHARE_GROUP_CAPACITY,
    describe_streams_group::DESCRIBE_STREAMS_GROUP_CAPACITY,
    legacy_alter_configs::LEGACY_ALTER_CONFIGS_CAPACITY,
    list_client_metrics_resources::internal_api::LIST_CLIENT_METRICS_RESOURCES_CAPACITY,
    list_config_resources::LIST_CONFIG_RESOURCES_CAPACITY,
    list_share_group_offsets::LIST_SHARE_GROUP_OFFSETS_CAPACITY,
    unregister_broker::UNREGISTER_BROKER_CAPACITY, update_features::UPDATE_FEATURES_CAPACITY,
};

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
};

pub(crate) use publishers::{
    AdminAbortPartitionTransactionPublisher, AdminAddRaftVoterPublisher,
    AdminAlterClientQuotasPublisher, AdminAlterReplicaLogDirsPublisher,
    AdminAlterShareGroupOffsetsPublisher, AdminAlterUserScramCredentialsPublisher,
    AdminCreateAclsPublisher, AdminCreateDelegationTokenPublisher, AdminDeleteAclsPublisher,
    AdminDeleteShareGroupOffsetsPublisher, AdminDescribeAclsPublisher,
    AdminDescribeClientQuotasPublisher, AdminDescribeConsumerGroupsPublisher,
    AdminDescribeDelegationTokensPublisher, AdminDescribeFeaturesPublisher,
    AdminDescribeLogDirsPublisher, AdminDescribeMetadataQuorumPublisher,
    AdminDescribeProducersPublisher, AdminDescribeReplicaLogDirsPublisher,
    AdminDescribeShareGroupPublisher, AdminDescribeStreamsGroupPublisher,
    AdminDescribeTopicPartitionsPublisher, AdminDescribeTransactionsPublisher,
    AdminDescribeUserScramCredentialsPublisher, AdminExpireDelegationTokenPublisher,
    AdminFenceProducersPublisher, AdminListClientMetricsResourcesPublisher,
    AdminListConfigResourcesPublisher, AdminListConsumerGroupsPublisher, AdminListOffsetsPublisher,
    AdminListShareGroupOffsetsPublisher, AdminListTransactionsPublisher,
    AdminRemoveRaftVoterPublisher, AdminRenewDelegationTokenPublisher,
    AdminUnregisterBrokerPublisher, AdminUpdateFeaturesPublisher,
    AlterConsumerGroupOffsetsPublisher, AlterPartitionReassignmentsPublisher,
    CreatePartitionsPublisher, CreateTopicsPublisher, DeleteConsumerGroupOffsetsPublisher,
    DeleteConsumerGroupsPublisher, DeleteRecordsPublisher, DeleteTopicsPublisher,
    DescribeClusterPublisher, DescribeConfigsPublisher, DescribeTopicsPublisher,
    ElectLeadersPublisher, IncrementalAlterConfigsPublisher, LegacyAlterConfigsPublisher,
    ListConsumerGroupOffsetsPublisher, ListPartitionReassignmentsPublisher,
    RemoveConsumerGroupMembersPublisher,
};

use super::{
    ADMIN_DESCRIBE_PRODUCERS_CAPACITY, ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY,
    ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY, ADMIN_FENCE_PRODUCERS_CAPACITY,
    ADMIN_LIST_OFFSETS_CAPACITY, ADMIN_LIST_TRANSACTIONS_CAPACITY, ALTER_CLIENT_QUOTAS_CAPACITY,
    ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY, ALTER_PARTITION_REASSIGNMENTS_CAPACITY,
    ALTER_REPLICA_LOG_DIRS_CAPACITY, ALTER_USER_SCRAM_CREDENTIALS_CAPACITY, CREATE_ACLS_CAPACITY,
    CREATE_PARTITIONS_CAPACITY, CREATE_TOPICS_CAPACITY, DELETE_ACLS_CAPACITY,
    DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY, DELETE_CONSUMER_GROUPS_CAPACITY,
    DELETE_RECORDS_CAPACITY, DELETE_TOPICS_CAPACITY, DESCRIBE_ACLS_CAPACITY,
    DESCRIBE_CLIENT_QUOTAS_CAPACITY, DESCRIBE_CLUSTER_CAPACITY, DESCRIBE_CONFIGS_CAPACITY,
    DESCRIBE_CONSUMER_GROUPS_CAPACITY, DESCRIBE_FEATURES_CAPACITY, DESCRIBE_LOG_DIRS_CAPACITY,
    DESCRIBE_METADATA_QUORUM_CAPACITY, DESCRIBE_REPLICA_LOG_DIRS_CAPACITY,
    DESCRIBE_TOPICS_CAPACITY, DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY, ELECT_LEADERS_CAPACITY,
    INCREMENTAL_ALTER_CONFIGS_CAPACITY, LIST_CONSUMER_GROUP_OFFSETS_CAPACITY,
    LIST_CONSUMER_GROUPS_CAPACITY, LIST_PARTITION_REASSIGNMENTS_CAPACITY,
    REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY,
};

const ADMIN_NOTIFIER_THREAD: &str = "kafka-client-admin-completion-notifier";
const ADMIN_NOTIFICATION_CAPACITY: usize = CREATE_TOPICS_CAPACITY
    + ABORT_PARTITION_TRANSACTION_CAPACITY
    + ADD_RAFT_VOTER_CAPACITY
    + REMOVE_RAFT_VOTER_CAPACITY
    + DELETE_TOPICS_CAPACITY
    + DESCRIBE_CLUSTER_CAPACITY
    + CREATE_PARTITIONS_CAPACITY
    + DESCRIBE_TOPICS_CAPACITY
    + DESCRIBE_CONFIGS_CAPACITY
    + INCREMENTAL_ALTER_CONFIGS_CAPACITY
    + LEGACY_ALTER_CONFIGS_CAPACITY
    + LIST_CONSUMER_GROUP_OFFSETS_CAPACITY
    + DELETE_CONSUMER_GROUP_OFFSETS_CAPACITY
    + DELETE_CONSUMER_GROUPS_CAPACITY
    + ALTER_CONSUMER_GROUP_OFFSETS_CAPACITY
    + ADMIN_LIST_OFFSETS_CAPACITY
    + LIST_PARTITION_REASSIGNMENTS_CAPACITY
    + ALTER_PARTITION_REASSIGNMENTS_CAPACITY
    + ELECT_LEADERS_CAPACITY
    + DELETE_RECORDS_CAPACITY
    + DELETE_SHARE_GROUP_OFFSETS_CAPACITY
    + LIST_SHARE_GROUP_OFFSETS_CAPACITY
    + ALTER_SHARE_GROUP_OFFSETS_CAPACITY
    + DESCRIBE_SHARE_GROUP_CAPACITY
    + DESCRIBE_STREAMS_GROUP_CAPACITY
    + DESCRIBE_CONSUMER_GROUPS_CAPACITY
    + DESCRIBE_FEATURES_CAPACITY
    + LIST_CONSUMER_GROUPS_CAPACITY
    + REMOVE_CONSUMER_GROUP_MEMBERS_CAPACITY
    + DESCRIBE_LOG_DIRS_CAPACITY
    + DESCRIBE_REPLICA_LOG_DIRS_CAPACITY
    + ALTER_REPLICA_LOG_DIRS_CAPACITY
    + DESCRIBE_ACLS_CAPACITY
    + DESCRIBE_CLIENT_QUOTAS_CAPACITY
    + ALTER_CLIENT_QUOTAS_CAPACITY
    + ALTER_USER_SCRAM_CREDENTIALS_CAPACITY
    + DESCRIBE_USER_SCRAM_CREDENTIALS_CAPACITY
    + DESCRIBE_METADATA_QUORUM_CAPACITY
    + ADMIN_DESCRIBE_PRODUCERS_CAPACITY
    + ADMIN_DESCRIBE_TOPIC_PARTITIONS_CAPACITY
    + ADMIN_DESCRIBE_TRANSACTIONS_CAPACITY
    + ADMIN_FENCE_PRODUCERS_CAPACITY
    + ADMIN_LIST_TRANSACTIONS_CAPACITY
    + LIST_CLIENT_METRICS_RESOURCES_CAPACITY
    + UPDATE_FEATURES_CAPACITY
    + UNREGISTER_BROKER_CAPACITY
    + LIST_CONFIG_RESOURCES_CAPACITY
    + CREATE_ACLS_CAPACITY
    + CREATE_DELEGATION_TOKEN_CAPACITY
    + DESCRIBE_DELEGATION_TOKENS_CAPACITY
    + RENEW_DELEGATION_TOKEN_CAPACITY
    + EXPIRE_DELEGATION_TOKEN_CAPACITY
    + DELETE_ACLS_CAPACITY;

/// Closed allocation-free set of terminal tickets accepted by the admin worker.
pub(crate) enum AdminPublishTicket {
    AbortPartitionTransaction(PublishTicket<AbortPartitionTransactionTerminal>),
    AddRaftVoter(PublishTicket<AddRaftVoterTerminal>),
    RemoveRaftVoter(PublishTicket<RemoveRaftVoterTerminal>),
    CreateTopics(PublishTicket<CreateTopicsTerminal>),
    DeleteTopics(PublishTicket<DeleteTopicsTerminal>),
    DescribeCluster(PublishTicket<DescribeClusterTerminal>),
    CreatePartitions(PublishTicket<CreatePartitionsTerminal>),
    DescribeTopics(PublishTicket<DescribeTopicsTerminal>),
    DescribeConfigs(PublishTicket<DescribeConfigsTerminal>),
    IncrementalAlterConfigs(PublishTicket<IncrementalAlterConfigsTerminal>),
    LegacyAlterConfigs(PublishTicket<LegacyAlterConfigsTerminal>),
    ListConsumerGroupOffsets(PublishTicket<ListConsumerGroupOffsetsTerminal>),
    DeleteConsumerGroupOffsets(PublishTicket<DeleteConsumerGroupOffsetsTerminal>),
    DeleteConsumerGroups(PublishTicket<DeleteConsumerGroupsTerminal>),
    AlterConsumerGroupOffsets(PublishTicket<AlterConsumerGroupOffsetsTerminal>),
    AdminListOffsets(PublishTicket<AdminListOffsetsTerminal>),
    ListPartitionReassignments(PublishTicket<ListPartitionReassignmentsTerminal>),
    AlterPartitionReassignments(PublishTicket<AlterPartitionReassignmentsTerminal>),
    ElectLeaders(PublishTicket<ElectLeadersTerminal>),
    DeleteRecords(PublishTicket<DeleteRecordsTerminal>),
    DeleteShareGroupOffsets(PublishTicket<DeleteShareGroupOffsetsTerminal>),
    ListShareGroupOffsets(PublishTicket<ListShareGroupOffsetsTerminal>),
    AlterShareGroupOffsets(PublishTicket<AlterShareGroupOffsetsTerminal>),
    DescribeShareGroup(PublishTicket<DescribeShareGroupTerminal>),
    DescribeStreamsGroup(PublishTicket<DescribeStreamsGroupTerminal>),
    DescribeConsumerGroups(PublishTicket<AdminDescribeConsumerGroupsTerminal>),
    DescribeFeatures(PublishTicket<DescribeFeaturesTerminal>),
    ListConsumerGroups(PublishTicket<AdminListConsumerGroupsTerminal>),
    RemoveConsumerGroupMembers(PublishTicket<RemoveConsumerGroupMembersTerminal>),
    DescribeLogDirs(PublishTicket<AdminDescribeLogDirsTerminal>),
    DescribeReplicaLogDirs(PublishTicket<DescribeReplicaLogDirsTerminal>),
    AlterReplicaLogDirs(PublishTicket<AlterReplicaLogDirsTerminal>),
    DescribeAcls(PublishTicket<DescribeAclsTerminal>),
    DescribeClientQuotas(PublishTicket<DescribeClientQuotasTerminal>),
    AlterClientQuotas(PublishTicket<AlterClientQuotasTerminal>),
    AlterUserScramCredentials(PublishTicket<AlterUserScramCredentialsTerminal>),
    DescribeUserScramCredentials(PublishTicket<DescribeUserScramCredentialsTerminal>),
    DescribeMetadataQuorum(PublishTicket<DescribeMetadataQuorumTerminal>),
    DescribeProducers(PublishTicket<AdminDescribeProducersTerminal>),
    DescribeTopicPartitions(PublishTicket<DescribeTopicPartitionsTerminal>),
    DescribeTransactions(PublishTicket<AdminDescribeTransactionsTerminal>),
    FenceProducers(PublishTicket<AdminFenceProducersTerminal>),
    ListTransactions(PublishTicket<AdminListTransactionsTerminal>),
    ListClientMetricsResources(PublishTicket<ListClientMetricsResourcesTerminal>),
    UpdateFeatures(PublishTicket<UpdateFeaturesTerminal>),
    UnregisterBroker(PublishTicket<UnregisterBrokerTerminal>),
    ListConfigResources(PublishTicket<ListConfigResourcesTerminal>),
    CreateAcls(PublishTicket<CreateAclsOutcome>),
    CreateDelegationToken(PublishTicket<CreateDelegationTokenTerminal>),
    DescribeDelegationTokens(PublishTicket<DescribeDelegationTokensTerminal>),
    RenewDelegationToken(PublishTicket<RenewDelegationTokenTerminal>),
    ExpireDelegationToken(PublishTicket<ExpireDelegationTokenTerminal>),
    DeleteAcls(PublishTicket<DeleteAclsOutcome>),
}

impl NotificationTicket for AdminPublishTicket {
    fn publish(self) {
        match self {
            Self::AbortPartitionTransaction(ticket) => ticket.publish(),
            Self::AddRaftVoter(ticket) => ticket.publish(),
            Self::RemoveRaftVoter(ticket) => ticket.publish(),
            Self::CreateTopics(ticket) => ticket.publish(),
            Self::DeleteTopics(ticket) => ticket.publish(),
            Self::DescribeCluster(ticket) => ticket.publish(),
            Self::CreatePartitions(ticket) => ticket.publish(),
            Self::DescribeTopics(ticket) => ticket.publish(),
            Self::DescribeConfigs(ticket) => ticket.publish(),
            Self::IncrementalAlterConfigs(ticket) => ticket.publish(),
            Self::LegacyAlterConfigs(ticket) => ticket.publish(),
            Self::ListConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::DeleteConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::DeleteConsumerGroups(ticket) => ticket.publish(),
            Self::AlterConsumerGroupOffsets(ticket) => ticket.publish(),
            Self::AdminListOffsets(ticket) => ticket.publish(),
            Self::ListPartitionReassignments(ticket) => ticket.publish(),
            Self::AlterPartitionReassignments(ticket) => ticket.publish(),
            Self::ElectLeaders(ticket) => ticket.publish(),
            Self::DeleteRecords(ticket) => ticket.publish(),
            Self::DeleteShareGroupOffsets(ticket) => ticket.publish(),
            Self::ListShareGroupOffsets(ticket) => ticket.publish(),
            Self::AlterShareGroupOffsets(ticket) => ticket.publish(),
            Self::DescribeShareGroup(ticket) => ticket.publish(),
            Self::DescribeStreamsGroup(ticket) => ticket.publish(),
            Self::DescribeConsumerGroups(ticket) => ticket.publish(),
            Self::DescribeFeatures(ticket) => ticket.publish(),
            Self::ListConsumerGroups(ticket) => ticket.publish(),
            Self::RemoveConsumerGroupMembers(ticket) => ticket.publish(),
            Self::DescribeLogDirs(ticket) => ticket.publish(),
            Self::DescribeReplicaLogDirs(ticket) => ticket.publish(),
            Self::AlterReplicaLogDirs(ticket) => ticket.publish(),
            Self::DescribeAcls(ticket) => ticket.publish(),
            Self::DescribeClientQuotas(ticket) => ticket.publish(),
            Self::AlterClientQuotas(ticket) => ticket.publish(),
            Self::AlterUserScramCredentials(ticket) => ticket.publish(),
            Self::DescribeUserScramCredentials(ticket) => ticket.publish(),
            Self::DescribeMetadataQuorum(ticket) => ticket.publish(),
            Self::DescribeProducers(ticket) => ticket.publish(),
            Self::DescribeTopicPartitions(ticket) => ticket.publish(),
            Self::DescribeTransactions(ticket) => ticket.publish(),
            Self::FenceProducers(ticket) => ticket.publish(),
            Self::ListTransactions(ticket) => ticket.publish(),
            Self::ListClientMetricsResources(ticket) => ticket.publish(),
            Self::UpdateFeatures(ticket) => ticket.publish(),
            Self::UnregisterBroker(ticket) => ticket.publish(),
            Self::ListConfigResources(ticket) => ticket.publish(),
            Self::CreateAcls(ticket) => ticket.publish(),
            Self::CreateDelegationToken(ticket) => ticket.publish(),
            Self::DescribeDelegationTokens(ticket) => ticket.publish(),
            Self::RenewDelegationToken(ticket) => ticket.publish(),
            Self::ExpireDelegationToken(ticket) => ticket.publish(),
            Self::DeleteAcls(ticket) => ticket.publish(),
        }
    }
}

/// Exact typed ports issued once with the shared worker.
pub(crate) struct AdminCompletionPorts {
    pub(crate) abort_partition_transaction: AdminAbortPartitionTransactionPublisher,
    pub(crate) add_raft_voter: AdminAddRaftVoterPublisher,
    pub(crate) remove_raft_voter: AdminRemoveRaftVoterPublisher,
    pub(crate) create_topics: CreateTopicsPublisher,
    pub(crate) delete_topics: DeleteTopicsPublisher,
    pub(crate) describe_cluster: DescribeClusterPublisher,
    pub(crate) create_partitions: CreatePartitionsPublisher,
    pub(crate) describe_topics: DescribeTopicsPublisher,
    pub(crate) describe_configs: DescribeConfigsPublisher,
    pub(crate) incremental_alter_configs: IncrementalAlterConfigsPublisher,
    pub(crate) legacy_alter_configs: LegacyAlterConfigsPublisher,
    pub(crate) list_consumer_group_offsets: ListConsumerGroupOffsetsPublisher,
    pub(crate) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsPublisher,
    pub(crate) delete_consumer_groups: DeleteConsumerGroupsPublisher,
    pub(crate) alter_consumer_group_offsets: AlterConsumerGroupOffsetsPublisher,
    pub(crate) admin_list_offsets: AdminListOffsetsPublisher,
    pub(crate) list_partition_reassignments: ListPartitionReassignmentsPublisher,
    pub(crate) alter_partition_reassignments: AlterPartitionReassignmentsPublisher,
    pub(crate) elect_leaders: ElectLeadersPublisher,
    pub(crate) delete_records: DeleteRecordsPublisher,
    pub(crate) delete_share_group_offsets: AdminDeleteShareGroupOffsetsPublisher,
    pub(crate) list_share_group_offsets: AdminListShareGroupOffsetsPublisher,
    pub(crate) alter_share_group_offsets: AdminAlterShareGroupOffsetsPublisher,
    pub(crate) describe_share_group: AdminDescribeShareGroupPublisher,
    pub(crate) describe_streams_group: AdminDescribeStreamsGroupPublisher,
    pub(crate) describe_consumer_groups: AdminDescribeConsumerGroupsPublisher,
    pub(crate) describe_features: AdminDescribeFeaturesPublisher,
    pub(crate) list_consumer_groups: AdminListConsumerGroupsPublisher,
    pub(crate) remove_consumer_group_members: RemoveConsumerGroupMembersPublisher,
    pub(crate) describe_log_dirs: AdminDescribeLogDirsPublisher,
    pub(crate) describe_replica_log_dirs: AdminDescribeReplicaLogDirsPublisher,
    pub(crate) alter_replica_log_dirs: AdminAlterReplicaLogDirsPublisher,
    pub(crate) describe_acls: AdminDescribeAclsPublisher,
    pub(crate) describe_client_quotas: AdminDescribeClientQuotasPublisher,
    pub(crate) alter_client_quotas: AdminAlterClientQuotasPublisher,
    pub(crate) alter_user_scram_credentials: AdminAlterUserScramCredentialsPublisher,
    pub(crate) describe_user_scram_credentials: AdminDescribeUserScramCredentialsPublisher,
    pub(crate) describe_metadata_quorum: AdminDescribeMetadataQuorumPublisher,
    pub(crate) describe_producers: AdminDescribeProducersPublisher,
    pub(crate) describe_topic_partitions: AdminDescribeTopicPartitionsPublisher,
    pub(crate) describe_transactions: AdminDescribeTransactionsPublisher,
    pub(crate) fence_producers: AdminFenceProducersPublisher,
    pub(crate) list_transactions: AdminListTransactionsPublisher,
    pub(crate) list_client_metrics_resources: AdminListClientMetricsResourcesPublisher,
    pub(crate) update_features: AdminUpdateFeaturesPublisher,
    pub(crate) unregister_broker: AdminUnregisterBrokerPublisher,
    pub(crate) list_config_resources: AdminListConfigResourcesPublisher,
    pub(crate) create_acls: AdminCreateAclsPublisher,
    pub(crate) create_delegation_token: AdminCreateDelegationTokenPublisher,
    pub(crate) describe_delegation_tokens: AdminDescribeDelegationTokensPublisher,
    pub(crate) renew_delegation_token: AdminRenewDelegationTokenPublisher,
    pub(crate) expire_delegation_token: AdminExpireDelegationTokenPublisher,
    pub(crate) delete_acls: AdminDeleteAclsPublisher,
}

/// Unique lifecycle owner for the one shared admin notifier.
pub(crate) struct AdminCompletionNotifier {
    worker: Option<SharedNotifier<AdminPublishTicket>>,
}

impl AdminCompletionNotifier {
    pub(crate) fn start() -> std::io::Result<(Self, AdminCompletionPorts)> {
        let worker = SharedNotifier::start(ADMIN_NOTIFICATION_CAPACITY, ADMIN_NOTIFIER_THREAD)?;
        let ports = AdminCompletionPorts {
            abort_partition_transaction: worker
                .publish_port(AdminPublishTicket::AbortPartitionTransaction),
            add_raft_voter: worker.publish_port(AdminPublishTicket::AddRaftVoter),
            remove_raft_voter: worker.publish_port(AdminPublishTicket::RemoveRaftVoter),
            create_topics: worker.publish_port(AdminPublishTicket::CreateTopics),
            delete_topics: worker.publish_port(AdminPublishTicket::DeleteTopics),
            describe_cluster: worker.publish_port(AdminPublishTicket::DescribeCluster),
            create_partitions: worker.publish_port(AdminPublishTicket::CreatePartitions),
            describe_topics: worker.publish_port(AdminPublishTicket::DescribeTopics),
            describe_configs: worker.publish_port(AdminPublishTicket::DescribeConfigs),
            incremental_alter_configs: worker
                .publish_port(AdminPublishTicket::IncrementalAlterConfigs),
            legacy_alter_configs: worker.publish_port(AdminPublishTicket::LegacyAlterConfigs),
            list_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::ListConsumerGroupOffsets),
            delete_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::DeleteConsumerGroupOffsets),
            delete_consumer_groups: worker.publish_port(AdminPublishTicket::DeleteConsumerGroups),
            alter_consumer_group_offsets: worker
                .publish_port(AdminPublishTicket::AlterConsumerGroupOffsets),
            admin_list_offsets: worker.publish_port(AdminPublishTicket::AdminListOffsets),
            list_partition_reassignments: worker
                .publish_port(AdminPublishTicket::ListPartitionReassignments),
            alter_partition_reassignments: worker
                .publish_port(AdminPublishTicket::AlterPartitionReassignments),
            elect_leaders: worker.publish_port(AdminPublishTicket::ElectLeaders),
            delete_records: worker.publish_port(AdminPublishTicket::DeleteRecords),
            delete_share_group_offsets: worker
                .publish_port(AdminPublishTicket::DeleteShareGroupOffsets),
            list_share_group_offsets: worker
                .publish_port(AdminPublishTicket::ListShareGroupOffsets),
            alter_share_group_offsets: worker
                .publish_port(AdminPublishTicket::AlterShareGroupOffsets),
            describe_share_group: worker.publish_port(AdminPublishTicket::DescribeShareGroup),
            describe_streams_group: worker.publish_port(AdminPublishTicket::DescribeStreamsGroup),
            describe_consumer_groups: worker
                .publish_port(AdminPublishTicket::DescribeConsumerGroups),
            describe_features: worker.publish_port(AdminPublishTicket::DescribeFeatures),
            list_consumer_groups: worker.publish_port(AdminPublishTicket::ListConsumerGroups),
            remove_consumer_group_members: worker
                .publish_port(AdminPublishTicket::RemoveConsumerGroupMembers),
            describe_log_dirs: worker.publish_port(AdminPublishTicket::DescribeLogDirs),
            describe_replica_log_dirs: worker
                .publish_port(AdminPublishTicket::DescribeReplicaLogDirs),
            alter_replica_log_dirs: worker.publish_port(AdminPublishTicket::AlterReplicaLogDirs),
            describe_acls: worker.publish_port(AdminPublishTicket::DescribeAcls),
            describe_client_quotas: worker.publish_port(AdminPublishTicket::DescribeClientQuotas),
            alter_client_quotas: worker.publish_port(AdminPublishTicket::AlterClientQuotas),
            alter_user_scram_credentials: worker
                .publish_port(AdminPublishTicket::AlterUserScramCredentials),
            describe_user_scram_credentials: worker
                .publish_port(AdminPublishTicket::DescribeUserScramCredentials),
            describe_metadata_quorum: worker
                .publish_port(AdminPublishTicket::DescribeMetadataQuorum),
            describe_producers: worker.publish_port(AdminPublishTicket::DescribeProducers),
            describe_topic_partitions: worker
                .publish_port(AdminPublishTicket::DescribeTopicPartitions),
            describe_transactions: worker.publish_port(AdminPublishTicket::DescribeTransactions),
            fence_producers: worker.publish_port(AdminPublishTicket::FenceProducers),
            list_transactions: worker.publish_port(AdminPublishTicket::ListTransactions),
            list_client_metrics_resources: worker
                .publish_port(AdminPublishTicket::ListClientMetricsResources),
            update_features: worker.publish_port(AdminPublishTicket::UpdateFeatures),
            unregister_broker: worker.publish_port(AdminPublishTicket::UnregisterBroker),
            list_config_resources: worker.publish_port(AdminPublishTicket::ListConfigResources),
            create_acls: worker.publish_port(AdminPublishTicket::CreateAcls),
            create_delegation_token: worker.publish_port(AdminPublishTicket::CreateDelegationToken),
            describe_delegation_tokens: worker
                .publish_port(AdminPublishTicket::DescribeDelegationTokens),
            renew_delegation_token: worker.publish_port(AdminPublishTicket::RenewDelegationToken),
            expire_delegation_token: worker.publish_port(AdminPublishTicket::ExpireDelegationToken),
            delete_acls: worker.publish_port(AdminPublishTicket::DeleteAcls),
        };
        Ok((
            Self {
                worker: Some(worker),
            },
            ports,
        ))
    }

    pub(crate) fn stop(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.take_join()
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(crate) fn take_join(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.worker.as_ref().and_then(SharedNotifier::thread_id)
    }

    #[cfg(test)]
    pub(super) const fn capacity_for_test() -> usize {
        ADMIN_NOTIFICATION_CAPACITY
    }
}
