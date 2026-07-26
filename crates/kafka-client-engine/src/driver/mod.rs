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
pub(crate) use rpc::classic_group;
pub(crate) use rpc::{
    CreatePartitionsCompletionFailure, CreateTopicsCompletionFailure,
    DeleteTopicsCompletionFailure, DescribeClusterCalls, DescribeClusterCompletionFailure,
    DescribeConfigsCalls, DescribeConfigsCompletionFailure, DescribeTopicsCalls,
    DescribeTopicsCompletionFailure, FetchBeginSettlementError, FetchCallAdmission,
    FetchCompletionObservation, FetchConfirmationError, FetchControlPending, FetchPoll,
    FetchRecovery, FetchRequestPreparationError, FetchTerminal, GroupOffsetCommitPoll,
    GroupOffsetCommitShutdownRecovery, GroupOffsetsCall, GroupOffsetsDriverFailureKind,
    GroupOffsetsTerminal, GroupOffsetsTerminalFact, IncrementalAlterConfigsCalls,
    IncrementalAlterConfigsCompletionFailure, PartitionFetchRequest, PositionAdmissionFailure,
    PositionCompletionFailure, PositionRequestPreparationError, PositionResolutionRequest,
    ProduceCompletionFailure, ProducerIdentityCompletionFailure, StaleFetchConfirmationError,
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountCall, TopicPartitionCountFact, TopicPartitionCountFailure,
    TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls, TrackedDeleteTopicsCalls,
    TrackedFetchCalls, TrackedGroupOffsetCommitCalls, TrackedPositionCalls, TrackedProduceCalls,
    TrackedProducerIdentityCalls, TransactionInitCall, TransactionInitDriverFailureKind,
    TransactionInitTerminal, TransactionInitTerminalFact, classify_fetch_admission,
    classify_fetch_request_error,
};
pub(crate) use rpc::{
    GroupOffsetAlterCall, GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminal,
    GroupOffsetAlterTerminalFact, RecoveredGroupOffsetAlterCall,
};
pub(crate) use rpc::{
    GroupOffsetDeleteCall, GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminal,
    GroupOffsetDeleteTerminalFact,
};
#[expect(
    unused_imports,
    reason = "temporary handoff surface for the adjacent group position execution slice"
)]
pub(crate) use rpc::{
    GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchAdmission,
    GroupPositionOffsetFetchAdmissionFailure, GroupPositionOffsetFetchBeginError,
    GroupPositionOffsetFetchCompletionFailureKind, GroupPositionOffsetFetchCompletionObservation,
    GroupPositionOffsetFetchConfirmationFailure, GroupPositionOffsetFetchKey,
    GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchRestoreFailure,
    GroupPositionOffsetFetchReturn, GroupPositionOffsetFetchReturnReason,
    GroupPositionOffsetFetchShutdownRecovery, GroupPositionOffsetFetchSubmitError,
    GroupPositionOffsetFetchTerminal, TrackedGroupPositionOffsetFetchCalls,
};
pub(crate) use wake::{ReactorWake, ReactorWakeError};
