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
mod producer;
mod readiness;
mod record;
mod security;
mod shutdown;
mod transaction;

pub use admin::{
    AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, Admin, AlterClientQuotas, AlterClientQuotasBuilder,
    AlterClientQuotasResult, AlterConsumerGroupOffsets, AlterConsumerGroupOffsetsBuilder,
    AlterConsumerGroupOffsetsResult, AlterPartitionReassignments,
    AlterPartitionReassignmentsBuilder, AlterPartitionReassignmentsResult, AlterReplicaLogDirs,
    AlterReplicaLogDirsBuilder, AlterReplicaLogDirsResult, BatchResult,
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ClientQuotaAlteration,
    ClientQuotaAlterationOperation, ClientQuotaEntity, ClientQuotaEntityComponent,
    ClientQuotaEntry, ClientQuotaFilterComponent, ClientQuotaMatch, ClientQuotaValue,
    ClusterBroker, ClusterDescription, ConfigAlteration, ConfigAlterationOperation, ConfigEntry,
    ConfigSynonym, ConsumerGroupAssignment, ConsumerGroupDescription,
    ConsumerGroupDescriptionDetails, ConsumerGroupListing, ConsumerGroupMember,
    ConsumerGroupMemberDetails, ConsumerGroupMemberRemoval, ConsumerGroupOffset,
    ConsumerGroupOffsetAlteration, ConsumerGroupTopicPartitions, ConsumerProtocolGroupDetails,
    ConsumerProtocolMemberDetails, CreateAclBrokerError, CreateAclOutcome, CreateAclResult,
    CreateAcls, CreateAclsBuilder, CreateAclsResult, CreatePartitions, CreatePartitionsBuilder,
    CreateTopics, CreateTopicsBuilder, DeleteAclBrokerError, DeleteAclFilterOutcome,
    DeleteAclFilterResult, DeleteAclMatchOutcome, DeleteAclMatchResult, DeleteAcls,
    DeleteAclsBuilder, DeleteAclsResult, DeleteConsumerGroupOffsets,
    DeleteConsumerGroupOffsetsBuilder, DeleteConsumerGroupOffsetsResult, DeleteConsumerGroups,
    DeleteConsumerGroupsBuilder, DeleteConsumerGroupsResult, DeleteRecords, DeleteRecordsBuilder,
    DeleteRecordsResult, DeleteRecordsResultInfo, DeleteRecordsTarget, DeleteTopics,
    DeleteTopicsBuilder, DescribeAcls, DescribeAclsBuilder, DescribeAclsResult,
    DescribeClientQuotas, DescribeClientQuotasBuilder, DescribeClientQuotasResult, DescribeCluster,
    DescribeClusterBuilder, DescribeConfigs, DescribeConfigsBuilder, DescribeConfigsResult,
    DescribeConsumerGroups, DescribeConsumerGroupsBuilder, DescribeConsumerGroupsResult,
    DescribeLogDirs, DescribeLogDirsBuilder, DescribeLogDirsResult, DescribeTopics,
    DescribeTopicsBuilder, DescribeUserScramCredentials, DescribeUserScramCredentialsBuilder,
    DescribeUserScramCredentialsResult, ElectLeaders, ElectLeadersBuilder, ElectLeadersResult,
    IncrementalAlterConfigs, IncrementalAlterConfigsBuilder, IncrementalAlterConfigsResult,
    LeaderElectionTarget, LeaderElectionType, ListConsumerGroupOffsets,
    ListConsumerGroupOffsetsBuilder, ListConsumerGroupOffsetsResult, ListConsumerGroups,
    ListConsumerGroupsBrokerError, ListConsumerGroupsBuilder, ListConsumerGroupsResult,
    ListOffsets, ListOffsetsBuilder, ListOffsetsQuery, ListOffsetsResult, ListOffsetsResultInfo,
    ListPartitionReassignments, ListPartitionReassignmentsBuilder,
    ListPartitionReassignmentsResult, ListTopics, ListTopicsBuilder, LogDirDescription,
    LogDirReplica, NewPartitions, NewTopic, OffsetSpec, PartitionReassignment,
    PartitionReassignmentChange, RemoveConsumerGroupMembers, RemoveConsumerGroupMembersBuilder,
    RemoveConsumerGroupMembersResult, ReplicaLogDirAssignment, ResourcePattern,
    ScramCredentialInfo, ScramMechanism, TopicConfigAlterations, TopicConfigQuery,
    TopicDescription, TopicPartitionDescription, TopicPartitionReplica,
};
pub use client::{Client, ClientBuilder};
pub use consumer::{
    AssignedConsumer, AssignedConsumerBuildError, AssignedConsumerBuilder, AssignedConsumerEvent,
    AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind, Checkpoint, CloseAssignedConsumer,
    CloseConsumer, CommitConsumerCheckpoint, Consumer, ConsumerAcknowledgeError,
    ConsumerAssignment, ConsumerAssignmentPartition, ConsumerBatch, ConsumerBuildError,
    ConsumerBuilder, ConsumerCloseAdmissionError, ConsumerCommitAdmissionError,
    ConsumerCommitError, ConsumerEvent, ConsumerHeader, ConsumerRecord, ConsumerRecords,
    ConsumerRevocation, GroupConsumerHeader, GroupConsumerRecord, GroupConsumerRecords,
    GroupMetadata, NextAssignedEvent, NextConsumerEvent, OffsetReset, ReadIsolation, RecordBatch,
    RecvAssignedBatch, RecvConsumerBatch, Seek, StartPosition, TopicPartition,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError};
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
mod shutdown_test;
#[cfg(test)]
mod silent_broker_test;
