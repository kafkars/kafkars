//! Concrete terminal-to-ticket publisher aliases for the shared admin notifier.

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

use crate::completion::SharedPublishPort;

use super::super::{CreateAclsOutcome, DeleteAclsOutcome};
use super::AdminPublishTicket;

pub(crate) type CreateTopicsPublisher = SharedPublishPort<CreateTopicsTerminal, AdminPublishTicket>;
pub(crate) type AdminAbortPartitionTransactionPublisher =
    SharedPublishPort<AbortPartitionTransactionTerminal, AdminPublishTicket>;
pub(crate) type AdminAddRaftVoterPublisher =
    SharedPublishPort<AddRaftVoterTerminal, AdminPublishTicket>;
pub(crate) type AdminRemoveRaftVoterPublisher =
    SharedPublishPort<RemoveRaftVoterTerminal, AdminPublishTicket>;
pub(crate) type DeleteTopicsPublisher = SharedPublishPort<DeleteTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeClusterPublisher =
    SharedPublishPort<DescribeClusterTerminal, AdminPublishTicket>;
pub(crate) type CreatePartitionsPublisher =
    SharedPublishPort<CreatePartitionsTerminal, AdminPublishTicket>;
pub(crate) type DescribeTopicsPublisher =
    SharedPublishPort<DescribeTopicsTerminal, AdminPublishTicket>;
pub(crate) type DescribeConfigsPublisher =
    SharedPublishPort<DescribeConfigsTerminal, AdminPublishTicket>;
pub(crate) type IncrementalAlterConfigsPublisher =
    SharedPublishPort<IncrementalAlterConfigsTerminal, AdminPublishTicket>;
pub(crate) type LegacyAlterConfigsPublisher =
    SharedPublishPort<LegacyAlterConfigsTerminal, AdminPublishTicket>;
pub(crate) type ListConsumerGroupOffsetsPublisher =
    SharedPublishPort<ListConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type DeleteConsumerGroupOffsetsPublisher =
    SharedPublishPort<DeleteConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type DeleteConsumerGroupsPublisher =
    SharedPublishPort<DeleteConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type AlterConsumerGroupOffsetsPublisher =
    SharedPublishPort<AlterConsumerGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminListOffsetsPublisher =
    SharedPublishPort<AdminListOffsetsTerminal, AdminPublishTicket>;
pub(crate) type ListPartitionReassignmentsPublisher =
    SharedPublishPort<ListPartitionReassignmentsTerminal, AdminPublishTicket>;
pub(crate) type AlterPartitionReassignmentsPublisher =
    SharedPublishPort<AlterPartitionReassignmentsTerminal, AdminPublishTicket>;
pub(crate) type ElectLeadersPublisher = SharedPublishPort<ElectLeadersTerminal, AdminPublishTicket>;
pub(crate) type DeleteRecordsPublisher =
    SharedPublishPort<DeleteRecordsTerminal, AdminPublishTicket>;
pub(crate) type AdminDeleteShareGroupOffsetsPublisher =
    SharedPublishPort<DeleteShareGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminListShareGroupOffsetsPublisher =
    SharedPublishPort<ListShareGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterShareGroupOffsetsPublisher =
    SharedPublishPort<AlterShareGroupOffsetsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeShareGroupPublisher =
    SharedPublishPort<DescribeShareGroupTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeStreamsGroupPublisher =
    SharedPublishPort<DescribeStreamsGroupTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeConsumerGroupsPublisher =
    SharedPublishPort<AdminDescribeConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeFeaturesPublisher =
    SharedPublishPort<DescribeFeaturesTerminal, AdminPublishTicket>;
pub(crate) type AdminListConsumerGroupsPublisher =
    SharedPublishPort<AdminListConsumerGroupsTerminal, AdminPublishTicket>;
pub(crate) type RemoveConsumerGroupMembersPublisher =
    SharedPublishPort<RemoveConsumerGroupMembersTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeLogDirsPublisher =
    SharedPublishPort<AdminDescribeLogDirsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeReplicaLogDirsPublisher =
    SharedPublishPort<DescribeReplicaLogDirsTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterReplicaLogDirsPublisher =
    SharedPublishPort<AlterReplicaLogDirsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeAclsPublisher =
    SharedPublishPort<DescribeAclsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeClientQuotasPublisher =
    SharedPublishPort<DescribeClientQuotasTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterClientQuotasPublisher =
    SharedPublishPort<AlterClientQuotasTerminal, AdminPublishTicket>;
pub(crate) type AdminAlterUserScramCredentialsPublisher =
    SharedPublishPort<AlterUserScramCredentialsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeUserScramCredentialsPublisher =
    SharedPublishPort<DescribeUserScramCredentialsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeMetadataQuorumPublisher =
    SharedPublishPort<DescribeMetadataQuorumTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeProducersPublisher =
    SharedPublishPort<AdminDescribeProducersTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeTopicPartitionsPublisher =
    SharedPublishPort<DescribeTopicPartitionsTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeTransactionsPublisher =
    SharedPublishPort<AdminDescribeTransactionsTerminal, AdminPublishTicket>;
pub(crate) type AdminFenceProducersPublisher =
    SharedPublishPort<AdminFenceProducersTerminal, AdminPublishTicket>;
pub(crate) type AdminListTransactionsPublisher =
    SharedPublishPort<AdminListTransactionsTerminal, AdminPublishTicket>;
pub(crate) type AdminListClientMetricsResourcesPublisher =
    SharedPublishPort<ListClientMetricsResourcesTerminal, AdminPublishTicket>;
pub(crate) type AdminUpdateFeaturesPublisher =
    SharedPublishPort<UpdateFeaturesTerminal, AdminPublishTicket>;
pub(crate) type AdminUnregisterBrokerPublisher =
    SharedPublishPort<UnregisterBrokerTerminal, AdminPublishTicket>;
pub(crate) type AdminListConfigResourcesPublisher =
    SharedPublishPort<ListConfigResourcesTerminal, AdminPublishTicket>;
pub(crate) type AdminCreateAclsPublisher = SharedPublishPort<CreateAclsOutcome, AdminPublishTicket>;
pub(crate) type AdminCreateDelegationTokenPublisher =
    SharedPublishPort<CreateDelegationTokenTerminal, AdminPublishTicket>;
pub(crate) type AdminDescribeDelegationTokensPublisher =
    SharedPublishPort<DescribeDelegationTokensTerminal, AdminPublishTicket>;
pub(crate) type AdminRenewDelegationTokenPublisher =
    SharedPublishPort<RenewDelegationTokenTerminal, AdminPublishTicket>;
pub(crate) type AdminExpireDelegationTokenPublisher =
    SharedPublishPort<ExpireDelegationTokenTerminal, AdminPublishTicket>;
pub(crate) type AdminDeleteAclsPublisher = SharedPublishPort<DeleteAclsOutcome, AdminPublishTicket>;
