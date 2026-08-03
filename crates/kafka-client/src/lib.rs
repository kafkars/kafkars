//! Idiomatic Rust facade over the shared reactor-native Kafka client engine.
//!
//! Immediate explicit-partition producer admission, stage-aware cancellation,
//! flush observation, atomic close-and-drain, batched topic mutation, and
//! bounded topic description, committed group-offset listing and deletion,
//! committed group-offset alteration, configuration description, incremental
//! configuration alteration, and transactional-owner initialization with
//! explicit begin, record send, commit, and abort form the implemented slices.
//! Later API domains remain design probes.
#![forbid(unsafe_code)]
mod admin;
mod bridge;
mod client;
mod consumer;
mod error;
mod metrics;
mod producer;
mod readiness;
mod record;
mod security;
mod shutdown;
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
    AssignedConsumer, AssignedConsumerBuildError, AssignedConsumerBuilder, AssignedConsumerEvent,
    AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind, Checkpoint, CheckpointBuilder,
    CheckpointMarkError, CheckpointMarkErrorKind, ClassicGroupAssignor, CloseAssignedConsumer,
    CloseConsumer, CommitConsumerCheckpoint, Consumer, ConsumerAcknowledgeError,
    ConsumerAssignment, ConsumerAssignmentPartition, ConsumerBatch, ConsumerBuildError,
    ConsumerBuilder, ConsumerCloseAdmissionError, ConsumerCommitAdmissionError,
    ConsumerCommitError, ConsumerControl, ConsumerEvent, ConsumerGroupProtocol, ConsumerHeader,
    ConsumerRecord, ConsumerRecords, ConsumerRevocation, GroupConsumerHeader, GroupConsumerRecord,
    GroupConsumerRecords, GroupMetadata, NextAssignedEvent, NextConsumerEvent, OffsetReset,
    ReadIsolation, RecordBatch, RecvAssignedBatch, RecvConsumerBatch, Seek, StartPosition,
    TopicPartition,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError, RetryAdvice};
pub use metrics::{
    CallMetrics, FailureMetrics, LatencyMetric, LatencyMetrics, MailboxMetrics, Metrics,
    MetricsSnapshot, ProducerMetrics,
};
pub use producer::{
    CancellationOutcome, CloseProducer, Compression, Delivery, Flush, Producer, ProducerBuilder,
    ProducerLimits, RecordMetadata, Send, SendBatch, SendBatchResult, TrySendError,
};
pub use readiness::Ready;
pub use record::{Header, Record};
pub use security::{Sasl, SaslMechanism, Security, Tls};
pub use shutdown::Shutdown;
pub use transaction::{
    AbortTransaction, CommitTransaction, InitializeTransactionalProducer, SendTransactionOffsets,
    SendTransactionRecord, Transaction, TransactionEndAdmissionError,
    TransactionOffsetsAdmissionError, TransactionSendAdmissionError, TransactionalProducer,
    TransactionalProducerBuilder, TransactionalProducerIdentity,
};
#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod readiness_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod silent_broker_test;
