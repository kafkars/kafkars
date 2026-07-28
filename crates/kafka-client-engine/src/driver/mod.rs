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
#[cfg(test)]
pub(crate) use rpc::exports::GroupPositionOffsetFetchTestPartition;
pub(crate) use rpc::exports::{
    AdminListOffsetsCall, AdminListOffsetsDriverFailureKind, AdminListOffsetsTerminal,
    AdminListOffsetsTerminalFact, AlterPartitionReassignmentsCall,
    AlterPartitionReassignmentsDriverFailureKind, AlterPartitionReassignmentsTerminal,
    AlterPartitionReassignmentsTerminalFact, ClassicGroupLeaveCall,
    ClassicGroupLeaveCompletionError, ClassicGroupLeaveDriverFailureKind,
    ClassicGroupLeaveResolution, ClassicGroupLeaveRoute, ClassicGroupPositionResetCall,
    ClassicGroupPositionResetCompletionError, ClassicGroupPositionResetOutcome,
    ClassicGroupPositionResetRoute, CreatePartitionsCompletionFailure,
    CreateTopicsCompletionFailure, DeleteTopicsCompletionFailure, DescribeClusterCalls,
    DescribeClusterCompletionFailure, DescribeConfigsCalls, DescribeConfigsCompletionFailure,
    DescribeTopicsCalls, DescribeTopicsCompletionFailure, FetchBeginSettlementError,
    FetchCallAdmission, FetchCompletionObservation, FetchConfirmationError, FetchControlPending,
    FetchPoll, FetchRecovery, FetchTerminal, GroupOffsetAlterCall,
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
    ProducerIdentityCompletionFailure, ProducerTopicViewCall, RecoveredAdminListOffsetsCall,
    RecoveredAlterPartitionReassignmentsCall, RecoveredGroupOffsetAlterCall,
    RecoveredListPartitionReassignmentsCall, StaleFetchConfirmationError,
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountCall, TopicPartitionCountFact, TopicPartitionCountFailure,
    TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls, TrackedDeleteTopicsCalls,
    TrackedFetchCalls, TrackedGroupOffsetCommitCalls, TrackedPositionCalls, TrackedProduceCalls,
    TrackedProducerIdentityCalls, TransactionInitCall, TransactionInitDriverFailureKind,
    TransactionInitTerminal, TransactionInitTerminalFact, classify_fetch_admission,
    classify_fetch_request_error,
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
    AlterReplicaLogDirsCall, AlterReplicaLogDirsDriverFailureKind, AlterReplicaLogDirsRawTerminal,
    AlterReplicaLogDirsTerminalFact, DeleteRecordsCall, DeleteRecordsDriverFailureKind,
    DeleteRecordsRawTerminal, DeleteRecordsTerminalFact, DescribeConsumerGroupsCall,
    DescribeConsumerGroupsDriverFailureKind, DescribeConsumerGroupsTerminal,
    DescribeConsumerGroupsTerminalFact, DescribeLogDirsCall, DescribeLogDirsDriverFailureKind,
    DescribeLogDirsRawTerminal, DescribeLogDirsTerminalFact, ElectLeadersCall,
    ElectLeadersDriverFailureKind, ElectLeadersTerminal, ElectLeadersTerminalFact,
    ListConsumerGroupsCall, ListConsumerGroupsDriverFailureKind, ListConsumerGroupsRawTerminal,
    ListConsumerGroupsRawTerminalFact, RecoveredAlterReplicaLogDirsCall,
    RecoveredDeleteRecordsCall, RecoveredDescribeConsumerGroupsCall, RecoveredDescribeLogDirsCall,
    RecoveredElectLeadersCall, RecoveredRemoveConsumerGroupMembersCall,
    RemoveConsumerGroupMembersCall, RemoveConsumerGroupMembersDriverFailureKind,
    RemoveConsumerGroupMembersTerminal, RemoveConsumerGroupMembersTerminalFact, classic_group,
    transaction_control, transaction_offsets, transaction_produce,
};
pub(crate) use security::{EngineSecurityError, ValidatedSecurity, validate as validate_security};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
