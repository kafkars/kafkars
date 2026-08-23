//! Unique ownership of the embedded `kafka-driver` reactor and its controls.

mod delivery;
#[cfg(test)]
mod delivery_test;
mod endpoint;
mod error;
pub(crate) mod owner;
#[cfg(test)]
mod owner_test;
mod rpc;
mod security;
#[cfg(test)]
mod security_test;
mod shutdown;
#[cfg(test)]
mod shutdown_test;
mod wake;
#[cfg(test)]
mod wake_test;
pub(crate) use delivery::{request_failure_delivery, request_failure_kind};
pub(crate) use endpoint::EndpointError;
pub(crate) use error::DriverOwnerError;
pub(crate) use owner::{DriverOwner, DriverTurn};
pub(crate) use rpc::exports::ConsumerGroupHeartbeatRoute;
#[cfg(test)]
pub(crate) use rpc::exports::GroupPositionOffsetFetchTestPartition;
pub(crate) use rpc::exports::{
    AdminListOffsetsCall, AdminListOffsetsDriverFailureKind, AdminListOffsetsTerminal,
    AdminListOffsetsTerminalFact, AlterPartitionReassignmentsCall,
    AlterPartitionReassignmentsDriverFailureKind, AlterPartitionReassignmentsTerminal,
    AlterPartitionReassignmentsTerminalFact, BrokerFetchCallAdmission, BrokerFetchCloseCall,
    BrokerFetchRouteCall, BrokerFetchRouteFailureKind, BrokerId, ClassicGroupLeaveCall,
    ClassicGroupLeaveCompletionError, ClassicGroupLeaveDriverFailureKind,
    ClassicGroupLeaveResolution, ClassicGroupLeaveRoute, ClassicGroupPositionResetCall,
    ClassicGroupPositionResetCompletionError, ClassicGroupPositionResetOutcome,
    ClassicGroupPositionResetRoute, CreatePartitionsCompletionFailure,
    CreateTopicsCompletionFailure, DeleteTopicsCompletionFailure, DescribeClusterCalls,
    DescribeClusterCompletionFailure, DescribeConfigsCalls, DescribeConfigsCompletionFailure,
    DescribeTopicsCalls, DescribeTopicsCompletionFailure, FetchBeginSettlementError,
    FetchCallAdmission, FetchCompletionObservation, FetchConfirmationError, FetchControlPending,
    FetchPoll, FetchRecovery, FetchRouteRefresh, FetchRouteRefreshPoll, FetchTerminal,
    ForgottenFetchCompletionFailure, ForgottenFetchConfirmation, ForgottenFetchRequest,
    ForgottenFetchSubmitFailureKind, ForgottenFetchTerminal, GroupOffsetAlterCall,
    GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminal, GroupOffsetAlterTerminalFact,
    GroupOffsetCommitPoll, GroupOffsetCommitRefreshPoll, GroupOffsetCommitShutdownRecovery,
    GroupOffsetDeleteCall, GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminal,
    GroupOffsetDeleteTerminalFact, GroupOffsetsCall, GroupOffsetsDriverFailureKind,
    GroupOffsetsTerminal, GroupOffsetsTerminalFact, IncrementalAlterConfigsCalls,
    IncrementalAlterConfigsCompletionFailure, ListOffsetsResolution,
    ListPartitionReassignmentsCall, ListPartitionReassignmentsDriverFailureKind,
    ListPartitionReassignmentsRawTerminal, ListPartitionReassignmentsTerminalFact,
    PartitionFetchRequest, PositionAdmissionFailure, PositionCompletionFailure,
    PositionRequestPreparationError, PositionResolutionRequest, ProduceCompletionFailure,
    ProduceRouteRefreshPoll, ProducerIdentityCompletionFailure, RecoveredAdminListOffsetsCall,
    RecoveredAlterPartitionReassignmentsCall, RecoveredGroupOffsetAlterCall,
    RecoveredGroupOffsetDeleteCall, RecoveredGroupOffsetsCall,
    RecoveredListPartitionReassignmentsCall, ShareFetchCall, ShareFetchCompletionErrorKind,
    ShareFetchDriverSubmitErrorKind, ShareFetchFailureKind, ShareFetchResolution, ShareFetchRoute,
    ShareFetchTerminalContext, StaleFetchConfirmationError, TopicPartitionCountAdmissionFailure,
    TopicPartitionCountAdmissionFailureKind, TopicPartitionCountCall, TopicPartitionCountFact,
    TopicPartitionCountFailure, TopicRouteView, TopicRouteViewCall, TrackedBrokerFetchCalls,
    TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls, TrackedDeleteTopicsCalls,
    TrackedFetchCalls, TrackedForgottenFetchCall, TrackedGroupOffsetCommitCalls,
    TrackedPositionCalls, TrackedProduceCalls, TrackedProducerIdentityCalls, TransactionInitCall,
    TransactionInitDriverFailureKind, TransactionInitPoll, TransactionInitTerminal,
    TransactionInitTerminalFact, classify_fetch_admission, classify_fetch_request_error,
};
#[expect(
    unused_imports,
    reason = "one closed position RPC adapter surface serves execution and ownership tests"
)]
pub(crate) use rpc::exports::{
    GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchAdmission,
    GroupPositionOffsetFetchAdmissionFailure, GroupPositionOffsetFetchBeginError,
    GroupPositionOffsetFetchCompletionFailureKind, GroupPositionOffsetFetchCompletionObservation,
    GroupPositionOffsetFetchCompletionRecovery, GroupPositionOffsetFetchConfirmationFailure,
    GroupPositionOffsetFetchDriverFailureKind, GroupPositionOffsetFetchKey,
    GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchRestoreFailure,
    GroupPositionOffsetFetchReturn, GroupPositionOffsetFetchReturnReason,
    GroupPositionOffsetFetchShutdownRecovery, GroupPositionOffsetFetchSubmitError,
    GroupPositionOffsetFetchTerminal, GroupPositionOffsetFetchTerminalFact,
    TrackedGroupPositionOffsetFetchCalls,
};
pub(crate) use rpc::{
    AbortPartitionTransactionCall, AbortPartitionTransactionDriverFailureKind,
    AbortPartitionTransactionRawTerminal, AbortPartitionTransactionTerminalFact, AddRaftVoterCall,
    AddRaftVoterDriverFailureKind, AddRaftVoterRawTerminal, AddRaftVoterTerminalFact,
    AlterShareGroupOffsetsCall, AlterShareGroupOffsetsDriverFailureKind,
    AlterShareGroupOffsetsTerminal, AlterShareGroupOffsetsTerminalFact, CreateDelegationTokenCall,
    CreateDelegationTokenDriverFailureKind, CreateDelegationTokenRawTerminal,
    CreateDelegationTokenTerminalFact, CreatePartitionsControllerRefreshPoll,
    DeleteShareGroupOffsetsCall, DeleteShareGroupOffsetsDriverFailureKind,
    DeleteShareGroupOffsetsTerminal, DeleteShareGroupOffsetsTerminalFact,
    DeleteTopicsControllerRefreshPoll, DescribeDelegationTokensCall,
    DescribeDelegationTokensDriverFailureKind, DescribeDelegationTokensRawTerminal,
    DescribeDelegationTokensTerminalFact, DescribeFeaturesCall, DescribeFeaturesDriverFailureKind,
    DescribeFeaturesRawTerminal, DescribeFeaturesTerminalFact, DescribeMetadataQuorumCall,
    DescribeMetadataQuorumDriverFailureKind, DescribeMetadataQuorumRawTerminal,
    DescribeMetadataQuorumTerminalFact, DescribeProducersCall, DescribeProducersDriverFailureKind,
    DescribeProducersRawTerminal, DescribeProducersTerminalFact, DescribeReplicaLogDirsCall,
    DescribeReplicaLogDirsDriverFailureKind, DescribeReplicaLogDirsRawTerminal,
    DescribeReplicaLogDirsTerminalFact, DescribeShareGroupCall,
    DescribeShareGroupDriverFailureKind, DescribeShareGroupTerminal,
    DescribeShareGroupTerminalFact, DescribeStreamsGroupCall,
    DescribeStreamsGroupDriverFailureKind, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupTerminalFact, DescribeTopicPartitionsCall,
    DescribeTopicPartitionsDriverFailureKind, DescribeTopicPartitionsRawTerminal,
    DescribeTopicPartitionsTerminalFact, DescribeTransactionsCall,
    DescribeTransactionsDriverFailureKind, DescribeTransactionsRawTerminal,
    DescribeTransactionsTerminalFact, ExpireDelegationTokenCall,
    ExpireDelegationTokenDriverFailureKind, ExpireDelegationTokenRawTerminal,
    ExpireDelegationTokenTerminalFact, LegacyAlterConfigsCall, LegacyAlterConfigsDriverFailureKind,
    LegacyAlterConfigsTerminal, LegacyAlterConfigsTerminalFact, ListClientMetricsResourcesCall,
    ListClientMetricsResourcesDriverFailureKind, ListClientMetricsResourcesRawTerminal,
    ListClientMetricsResourcesTerminalFact, ListConfigResourcesCall,
    ListConfigResourcesDriverFailureKind, ListConfigResourcesRawTerminal,
    ListConfigResourcesTerminalFact, ListShareGroupOffsetsCall,
    ListShareGroupOffsetsDriverFailureKind, ListShareGroupOffsetsTerminal,
    ListShareGroupOffsetsTerminalFact, ListTransactionsCall, ListTransactionsDriverFailureKind,
    ListTransactionsRawTerminal, ListTransactionsRawTerminalFact, RecoveredAddRaftVoterCall,
    RecoveredCreateDelegationTokenCall, RecoveredDescribeDelegationTokensCall,
    RecoveredExpireDelegationTokenCall, RecoveredRemoveRaftVoterCall,
    RecoveredRenewDelegationTokenCall, RemoveRaftVoterCall, RemoveRaftVoterDriverFailureKind,
    RemoveRaftVoterRawTerminal, RemoveRaftVoterTerminalFact, RenewDelegationTokenCall,
    RenewDelegationTokenDriverFailureKind, RenewDelegationTokenRawTerminal,
    RenewDelegationTokenTerminalFact, UnregisterBrokerCall, UnregisterBrokerDriverFailureKind,
    UnregisterBrokerRawTerminal, UnregisterBrokerTerminalFact, UpdateFeaturesCall,
    UpdateFeaturesControllerRefreshPoll, UpdateFeaturesDriverFailureKind,
    UpdateFeaturesRawTerminal, UpdateFeaturesTerminalFact,
};
pub(crate) use rpc::{
    AlterClientQuotasCall, AlterClientQuotasDriverFailureKind, AlterClientQuotasRawTerminal,
    AlterClientQuotasTerminalFact, AlterReplicaLogDirsCall, AlterReplicaLogDirsDriverFailureKind,
    AlterReplicaLogDirsRawTerminal, AlterReplicaLogDirsTerminalFact, AlterUserScramCredentialsCall,
    AlterUserScramCredentialsDriverFailureKind, AlterUserScramCredentialsRawTerminal,
    AlterUserScramCredentialsTerminalFact, ConsumerGroupDescribeDriverFailureKind,
    ConsumerGroupDescribeTerminalFact, ConsumerGroupHeartbeatCall,
    ConsumerGroupHeartbeatCompletionError, ConsumerGroupHeartbeatDriverFailureKind,
    ConsumerGroupHeartbeatResolution, ConsumerGroupHeartbeatSubmitErrorKind, CreateAclsCall,
    CreateAclsDriverFailureKind, CreateAclsRawTerminal, CreateAclsTerminalFact, DeleteAclsCall,
    DeleteAclsDriverFailureKind, DeleteAclsRawTerminal, DeleteAclsTerminalFact,
    DeleteConsumerGroupsCall, DeleteConsumerGroupsDriverFailureKind,
    DeleteConsumerGroupsRawTerminal, DeleteConsumerGroupsTerminalFact, DeleteRecordsCall,
    DeleteRecordsDriverFailureKind, DeleteRecordsRawTerminal, DeleteRecordsTerminalFact,
    DescribeAclsCall, DescribeAclsDriverFailureKind, DescribeAclsRawTerminal,
    DescribeAclsTerminalFact, DescribeClientQuotasCall, DescribeClientQuotasDriverFailureKind,
    DescribeClientQuotasRawTerminal, DescribeClientQuotasTerminalFact, DescribeConsumerGroupsCall,
    DescribeConsumerGroupsDriverFailureKind, DescribeConsumerGroupsTerminal,
    DescribeConsumerGroupsTerminalFact, DescribeLogDirsCall, DescribeLogDirsDriverFailureKind,
    DescribeLogDirsRawTerminal, DescribeLogDirsTerminalFact, DescribeUserScramCredentialsCall,
    DescribeUserScramCredentialsDriverFailureKind, DescribeUserScramCredentialsRawTerminal,
    DescribeUserScramCredentialsTerminalFact, ElectLeadersCall, ElectLeadersControllerRefreshPoll,
    ElectLeadersDriverFailureKind, ElectLeadersTerminal, ElectLeadersTerminalFact,
    ListConsumerGroupsCall, ListConsumerGroupsDriverFailureKind, ListConsumerGroupsRawTerminal,
    ListConsumerGroupsRawTerminalFact, RecoveredAbortPartitionTransactionCall,
    RecoveredAlterClientQuotasCall, RecoveredAlterReplicaLogDirsCall,
    RecoveredAlterShareGroupOffsetsCall, RecoveredAlterUserScramCredentialsCall,
    RecoveredCreateAclsCall, RecoveredDeleteAclsCall, RecoveredDeleteConsumerGroupsCall,
    RecoveredDeleteRecordsCall, RecoveredDeleteShareGroupOffsetsCall, RecoveredDescribeAclsCall,
    RecoveredDescribeClientQuotasCall, RecoveredDescribeConsumerGroupsCall,
    RecoveredDescribeFeaturesCall, RecoveredDescribeLogDirsCall,
    RecoveredDescribeMetadataQuorumCall, RecoveredDescribeProducersCall,
    RecoveredDescribeReplicaLogDirsCall, RecoveredDescribeShareGroupCall,
    RecoveredDescribeStreamsGroupCall, RecoveredDescribeTopicPartitionsCall,
    RecoveredDescribeTransactionsCall, RecoveredDescribeUserScramCredentialsCall,
    RecoveredElectLeadersCall, RecoveredLegacyAlterConfigsCall,
    RecoveredListClientMetricsResourcesCall, RecoveredListConfigResourcesCall,
    RecoveredListShareGroupOffsetsCall, RecoveredRemoveConsumerGroupMembersCall,
    RecoveredUnregisterBrokerCall, RecoveredUpdateFeaturesCall, RemoveConsumerGroupMembersCall,
    RemoveConsumerGroupMembersDriverFailureKind, RemoveConsumerGroupMembersTerminal,
    RemoveConsumerGroupMembersTerminalFact,
};
pub(crate) use rpc::{classic_group, share_group_heartbeat};
pub(crate) use rpc::{transaction_control, transaction_offsets, transaction_produce};
pub(crate) use security::{EngineSecurityError, ValidatedSecurity, validate as validate_security};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
