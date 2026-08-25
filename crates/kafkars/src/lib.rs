//! Experimental, runtime-neutral Rust client with bounded deterministic operations.
//! Version 0.0.1 is an API preview, not a broker-compatibility claim.
//! Apache Kafka and the Kafka logo are trademarks of The Apache Software Foundation.
//! kafkars has no affiliation with or endorsement from the Foundation.
#![forbid(unsafe_code)]
mod admin;
mod bridge;
mod client;
mod consumer;
mod error;
mod header_name;
mod metrics;
mod producer;
mod public_api;
mod readiness;
mod record;
mod security;
mod shutdown;
mod topic_uuid;
mod transaction;
pub use admin::{
    AbortPartitionTransaction, AbortTransactionBuilder, AbortTransactionSpec, AccessControlEntry,
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AddRaftVoter, AddRaftVoterBuilder, AddRaftVoterResult, Admin, AlterClientQuotas,
    AlterClientQuotasBuilder, AlterClientQuotasResult, AlterConsumerGroupOffsets,
    AlterConsumerGroupOffsetsBuilder, AlterConsumerGroupOffsetsResult, AlterPartitionReassignments,
    AlterPartitionReassignmentsBuilder, AlterPartitionReassignmentsResult, AlterReplicaLogDirs,
    AlterReplicaLogDirsBuilder, AlterReplicaLogDirsResult, AlterShareGroupOffsets,
    AlterShareGroupOffsetsBuilder, AlterShareGroupOffsetsResult, AlterStreamsGroupOffsets,
    AlterStreamsGroupOffsetsBuilder, AlterStreamsGroupOffsetsResult, AlterUserScramCredentials,
    AlterUserScramCredentialsBuilder, AlterUserScramCredentialsResult, BatchResult,
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ClassicGroupDescription,
    ClassicGroupMember, ClientQuotaAlteration, ClientQuotaAlterationOperation, ClientQuotaEntity,
    ClientQuotaEntityComponent, ClientQuotaEntry, ClientQuotaFilterComponent, ClientQuotaMatch,
    ClientQuotaValue, ClusterBroker, ClusterDescription, ConfigAlteration,
    ConfigAlterationOperation, ConfigEntry, ConfigResource, ConfigResourceAlterations,
    ConfigResourceQuery, ConfigResourceType, ConfigSynonym, ConsumerGroupAssignment,
    ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupListing,
    ConsumerGroupMember, ConsumerGroupMemberDetails, ConsumerGroupMemberRemoval,
    ConsumerGroupOffset, ConsumerGroupOffsetAlteration, ConsumerGroupTopicPartitions,
    ConsumerProtocolGroupDetails, ConsumerProtocolMemberDetails, CreateAclBrokerError,
    CreateAclOutcome, CreateAclResult, CreateAcls, CreateAclsBuilder, CreateAclsResult,
    CreateDelegationToken, CreateDelegationTokenBuilder, CreateDelegationTokenResult,
    CreatePartitions, CreatePartitionsBuilder, CreateTopics, CreateTopicsBuilder, DelegationToken,
    DelegationTokenHmac, DelegationTokenHmacError, DelegationTokenPrincipal, DeleteAclBrokerError,
    DeleteAclFilterOutcome, DeleteAclFilterResult, DeleteAclMatchOutcome, DeleteAclMatchResult,
    DeleteAcls, DeleteAclsBuilder, DeleteAclsResult, DeleteConsumerGroupOffsets,
    DeleteConsumerGroupOffsetsBuilder, DeleteConsumerGroupOffsetsResult, DeleteConsumerGroups,
    DeleteConsumerGroupsBuilder, DeleteConsumerGroupsResult, DeleteRecords, DeleteRecordsBuilder,
    DeleteRecordsResult, DeleteRecordsResultInfo, DeleteRecordsTarget, DeleteShareGroupOffsets,
    DeleteShareGroupOffsetsBuilder, DeleteShareGroupOffsetsResult, DeleteShareGroups,
    DeleteShareGroupsBuilder, DeleteShareGroupsResult, DeleteStreamsGroupOffsets,
    DeleteStreamsGroupOffsetsBuilder, DeleteStreamsGroupOffsetsResult, DeleteStreamsGroups,
    DeleteStreamsGroupsBuilder, DeleteStreamsGroupsResult, DeleteTopics, DeleteTopicsBuilder,
    DeleteTopicsById, DeleteTopicsByIdBuilder, DescribeAcls, DescribeAclsBuilder,
    DescribeAclsResult, DescribeClassicGroups, DescribeClassicGroupsBuilder,
    DescribeClassicGroupsResult, DescribeClientQuotas, DescribeClientQuotasBuilder,
    DescribeClientQuotasResult, DescribeCluster, DescribeClusterBuilder, DescribeConfigResources,
    DescribeConfigResourcesBuilder, DescribeConfigResourcesResult, DescribeConfigs,
    DescribeConfigsBuilder, DescribeConfigsResult, DescribeConsumerGroups,
    DescribeConsumerGroupsBuilder, DescribeConsumerGroupsResult, DescribeDelegationTokens,
    DescribeDelegationTokensBuilder, DescribeDelegationTokensResult, DescribeFeatures,
    DescribeFeaturesBuilder, DescribeFeaturesResult, DescribeLogDirs, DescribeLogDirsBuilder,
    DescribeLogDirsResult, DescribeMetadataQuorum, DescribeMetadataQuorumBuilder,
    DescribeProducers, DescribeProducersBuilder, DescribeProducersResult, DescribeReplicaLogDirs,
    DescribeReplicaLogDirsBuilder, DescribeReplicaLogDirsResult, DescribeShareGroup,
    DescribeShareGroupBuilder, DescribeShareGroupResult, DescribeShareGroups,
    DescribeShareGroupsBuilder, DescribeShareGroupsResult, DescribeStreamsGroup,
    DescribeStreamsGroupBuilder, DescribeStreamsGroupResult, DescribeStreamsGroups,
    DescribeStreamsGroupsBuilder, DescribeStreamsGroupsResult, DescribeTopicPartition,
    DescribeTopicPartitions, DescribeTopicPartitionsBuilder, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsPage, DescribeTopicPartitionsTopic, DescribeTopics,
    DescribeTopicsBuilder, DescribeTopicsById, DescribeTopicsByIdBuilder, DescribeTransactions,
    DescribeTransactionsBuilder, DescribeTransactionsResult, DescribeUserScramCredentials,
    DescribeUserScramCredentialsBuilder, DescribeUserScramCredentialsResult, ElectLeaders,
    ElectLeadersBuilder, ElectLeadersResult, ExpireDelegationToken, ExpireDelegationTokenBuilder,
    ExpireDelegationTokenResult, FeatureUpdate, FeatureUpdateIntent, FenceProducers,
    FenceProducersBuilder, FenceProducersResult, FencedProducerIdentity, FinalizedFeature,
    ForceTerminateTransaction, ForceTerminateTransactionBuilder, GroupListing,
    IncrementalAlterConfigResources, IncrementalAlterConfigResourcesBuilder,
    IncrementalAlterConfigResourcesResult, IncrementalAlterConfigs, IncrementalAlterConfigsBuilder,
    IncrementalAlterConfigsResult, LeaderElectionTarget, LeaderElectionType,
    LegacyConfigResourceReplacement, LegacyReplaceConfigResources,
    LegacyReplaceConfigResourcesBuilder, LegacyReplaceConfigResourcesResult,
    LegacyReplaceTopicConfigs, LegacyReplaceTopicConfigsBuilder, LegacyReplaceTopicConfigsResult,
    LegacyTopicConfigEntry, LegacyTopicConfigReplacement, ListClientMetricsResources,
    ListClientMetricsResourcesBuilder, ListClientMetricsResourcesResult, ListConfigResources,
    ListConfigResourcesBuilder, ListConfigResourcesResult, ListConsumerGroupOffsets,
    ListConsumerGroupOffsetsBuilder, ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsResult,
    ListConsumerGroups, ListConsumerGroupsBrokerError, ListConsumerGroupsBuilder,
    ListConsumerGroupsOffsets, ListConsumerGroupsOffsetsBuilder, ListConsumerGroupsOffsetsResult,
    ListConsumerGroupsResult, ListGroups, ListGroupsBrokerError, ListGroupsBuilder,
    ListGroupsResult, ListOffsets, ListOffsetsBuilder, ListOffsetsQuery, ListOffsetsResult,
    ListOffsetsResultInfo, ListPartitionReassignments, ListPartitionReassignmentsBuilder,
    ListPartitionReassignmentsResult, ListShareGroupOffsets, ListShareGroupOffsetsBuilder,
    ListShareGroupOffsetsQuery, ListShareGroupOffsetsResult, ListShareGroupsOffsets,
    ListShareGroupsOffsetsBuilder, ListShareGroupsOffsetsResult, ListStreamsGroupOffsets,
    ListStreamsGroupOffsetsBuilder, ListStreamsGroupOffsetsQuery, ListStreamsGroupOffsetsResult,
    ListStreamsGroupsOffsets, ListStreamsGroupsOffsetsBuilder, ListStreamsGroupsOffsetsResult,
    ListTopics, ListTopicsBuilder, ListTransactions, ListTransactionsBrokerError,
    ListTransactionsBuilder, ListTransactionsResult, LogDirDescription, LogDirReplica,
    MetadataQuorumDescription, MetadataQuorumListener, MetadataQuorumNode, MetadataQuorumReplica,
    NewPartitions, NewTopic, NewTopicPlacement, OffsetSpec, PartitionReassignment,
    PartitionReassignmentChange, ProducerState, RaftVoterEndpoint, RaftVoterIdentity,
    RemoveConsumerGroupMembers, RemoveConsumerGroupMembersBuilder,
    RemoveConsumerGroupMembersResult, RemoveRaftVoter, RemoveRaftVoterBuilder,
    RemoveRaftVoterResult, RenewDelegationToken, RenewDelegationTokenBuilder,
    RenewDelegationTokenResult, ReplicaLogDirAssignment, ReplicaLogDirInfo, ReplicaLogDirLocation,
    ResourcePattern, ScramCredentialInfo, ScramMechanism, ShareGroupAssignment,
    ShareGroupDescription, ShareGroupMember, ShareGroupOffset, ShareGroupOffsetAlteration,
    ShareGroupTopicPartitions, StreamsGroupAssignment, StreamsGroupDescription,
    StreamsGroupEndpoint, StreamsGroupKeyValue, StreamsGroupMember, StreamsGroupSubtopology,
    StreamsGroupTaskIds, StreamsGroupTaskOffset, StreamsGroupTopicInfo, StreamsGroupTopology,
    StreamsGroupTopologyDescription, StreamsGroupTopologyDescriptionStatus,
    StreamsGroupTopologyDescriptionSubtopology, StreamsGroupTopologyGlobalStore,
    StreamsGroupTopologyNode, StreamsGroupTopologyNodeType, SupportedFeature,
    TopicConfigAlterations, TopicConfigQuery, TopicDescription, TopicPartitionDescription,
    TopicPartitionReplica, TopicReplicaAssignment, TransactionDescription, TransactionListing,
    TransactionTopic, UnregisterBroker, UnregisterBrokerBuilder, UnregisterBrokerResult,
    UpdateFeatures, UpdateFeaturesBuilder, UpdateFeaturesResult, UserScramCredentialAlteration,
};
pub use client::{Client, ClientBuilder};
pub use consumer::{
    AcknowledgeShareConsumer, AssignedConsumer, AssignedConsumerBuildError,
    AssignedConsumerBuilder, AssignedConsumerEvent, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerFetchThrottleFailureKind,
    AssignedConsumerPositionFence, AssignedConsumerPositionResolutionFailureKind, Checkpoint,
    CheckpointBuilder, CheckpointMarkError, CheckpointMarkErrorKind, ClassicGroupAssignor,
    ClassicGroupConfig, CloseAssignedConsumer, CloseConsumer, CloseShareConsumer,
    CommitConsumerCheckpoint, Consumer, ConsumerAcknowledgeError, ConsumerAssignment,
    ConsumerAssignmentPartition, ConsumerBatch, ConsumerBuildError, ConsumerBuilder,
    ConsumerCloseAdmissionError, ConsumerCommitAdmissionError, ConsumerCommitError,
    ConsumerControl, ConsumerEvent, ConsumerFetchConfig, ConsumerFetchEvidence,
    ConsumerGroupProtocol, ConsumerHeader, ConsumerLimits, ConsumerRecord, ConsumerRecords,
    ConsumerRevocation, GroupConsumerHeader, GroupConsumerOperationConfig, GroupConsumerRecord,
    GroupConsumerRecords, GroupMembershipEpoch, GroupMetadata, NextAssignedEvent,
    NextConsumerEvent, OffsetReset, OwnedConsumerBatch, OwnedConsumerHeader, OwnedConsumerRecord,
    OwnedConsumerRecords, ReadIsolation, RecordBatch, RecvAssignedBatch, RecvConsumerBatch,
    RecvShareConsumerBatch, Seek, ShareAcknowledgement, ShareAcknowledgementAdmissionError,
    ShareAcknowledgementBrokerError, ShareAcknowledgementBuildError,
    ShareAcknowledgementBuildErrorKind, ShareAcknowledgementError,
    ShareAcknowledgementPartitionOutcome, ShareAcknowledgementResponse, ShareConsumer,
    ShareConsumerAssignment, ShareConsumerAssignmentPartition, ShareConsumerBatch,
    ShareConsumerBuildError, ShareConsumerBuilder, ShareConsumerCloseAdmissionError,
    ShareConsumerFetchConfig, ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords,
    ShareDisposition, ShareRecordDecision, StartPosition, TopicPartition,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError, RetryAdvice};
pub use header_name::HeaderName;
pub use metrics::{
    CallMetrics, FailureMetrics, LatencyMetric, LatencyMetrics, MailboxMetrics, Metrics,
    MetricsSnapshot, ProducerMetrics,
};
pub use producer::{
    CancellationOutcome, CloseProducer, Compression, Delivery, Flush, Producer, ProducerBuilder,
    ProducerConfig, ProducerLimits, ProducerRetryConfig, RecordMetadata, Send, SendBatch,
    SendBatchResult, TrySendError,
};
pub use public_api::*;
pub use readiness::Ready;
pub use record::{Header, Record};
pub use security::{Sasl, SaslMechanism, Security, Tls};
pub use shutdown::Shutdown;
pub use topic_uuid::TopicUuid;
#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod header_name_test;
#[cfg(test)]
mod readiness_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod silent_broker_test;
#[cfg(test)]
mod topic_uuid_test;
