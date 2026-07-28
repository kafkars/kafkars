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
pub(crate) use rpc::GroupPositionOffsetFetchTestPartition;
pub(crate) use rpc::{
    AdminListOffsetsCall, AdminListOffsetsDriverFailureKind, AdminListOffsetsTerminal,
    AdminListOffsetsTerminalFact, AlterPartitionReassignmentsCall,
    AlterPartitionReassignmentsDriverFailureKind, AlterPartitionReassignmentsTerminal,
    AlterPartitionReassignmentsTerminalFact, CreatePartitionsCompletionFailure,
    CreateTopicsCompletionFailure, DeleteTopicsCompletionFailure, DescribeClusterCalls,
    DescribeClusterCompletionFailure, DescribeConfigsCalls, DescribeConfigsCompletionFailure,
    DescribeTopicsCalls, DescribeTopicsCompletionFailure, FetchBeginSettlementError,
    FetchCallAdmission, FetchCompletionObservation, FetchConfirmationError, FetchControlPending,
    FetchPoll, FetchRecovery, FetchTerminal, GroupOffsetAlterCall,
    GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminal, GroupOffsetAlterTerminalFact,
    GroupOffsetCommitPoll, GroupOffsetCommitShutdownRecovery, GroupOffsetDeleteCall,
    GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminal, GroupOffsetDeleteTerminalFact,
    GroupOffsetsCall, GroupOffsetsDriverFailureKind, GroupOffsetsTerminal,
    GroupOffsetsTerminalFact, IncrementalAlterConfigsCalls,
    IncrementalAlterConfigsCompletionFailure, ListPartitionReassignmentsCall,
    ListPartitionReassignmentsDriverFailureKind, ListPartitionReassignmentsRawTerminal,
    ListPartitionReassignmentsTerminalFact, PartitionFetchRequest, PositionAdmissionFailure,
    PositionCompletionFailure, PositionRequestPreparationError, PositionResolutionRequest,
    ProduceCompletionFailure, ProducerIdentityCompletionFailure, ProducerTopicViewCall,
    RecoveredAdminListOffsetsCall, RecoveredAlterPartitionReassignmentsCall,
    RecoveredGroupOffsetAlterCall, RecoveredListPartitionReassignmentsCall,
    StaleFetchConfirmationError, TopicPartitionCountAdmissionFailure,
    TopicPartitionCountAdmissionFailureKind, TopicPartitionCountCall, TopicPartitionCountFact,
    TopicPartitionCountFailure, TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls,
    TrackedDeleteTopicsCalls, TrackedFetchCalls, TrackedGroupOffsetCommitCalls,
    TrackedPositionCalls, TrackedProduceCalls, TrackedProducerIdentityCalls, TransactionInitCall,
    TransactionInitDriverFailureKind, TransactionInitTerminal, TransactionInitTerminalFact,
    classic_group, classify_fetch_admission, classify_fetch_request_error, transaction_control,
};
#[expect(
    unused_imports,
    reason = "one closed position RPC adapter surface serves execution and ownership tests"
)]
pub(crate) use rpc::{
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
pub(crate) use security::{EngineSecurityError, ValidatedSecurity, validate as validate_security};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
