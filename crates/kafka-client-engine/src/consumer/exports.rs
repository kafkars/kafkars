//! Crate-private assigned-consumer capabilities exposed to the engine host.

pub use super::assigned_host::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerAssignment,
    AssignedConsumerAssignmentCapture, AssignedConsumerAssignmentEpoch,
    AssignedConsumerAssignmentInputError, AssignedConsumerAssignmentInputErrorKind,
    AssignedConsumerBatch, AssignedConsumerClaimError, AssignedConsumerCloseObserver,
    AssignedConsumerCloseObserverError, AssignedConsumerControlAccepted,
    AssignedConsumerControlError, AssignedConsumerControlErrorKind, AssignedConsumerEvent,
    AssignedConsumerFetchFailure, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailure, AssignedConsumerFetchThrottleFailureKind,
    AssignedConsumerHandle, AssignedConsumerHeader, AssignedConsumerNextEvent,
    AssignedConsumerNextEventError, AssignedConsumerNextEventErrorKind, AssignedConsumerPartition,
    AssignedConsumerPartitionInputError, AssignedConsumerPartitionInputErrorKind,
    AssignedConsumerPositionFence, AssignedConsumerPositionResolutionFailure,
    AssignedConsumerPositionResolutionFailureKind, AssignedConsumerRecord, AssignedConsumerRecords,
    AssignedConsumerRecv, AssignedConsumerRecvError, AssignedConsumerRecvErrorKind,
    AssignedConsumerResumeCapture, AssignedConsumerSeekCapture, AssignedConsumerStartPosition,
    AssignedConsumerTryCloseAccepted, AssignedConsumerTryCloseError,
    AssignedConsumerTryCloseErrorKind, AssignedConsumerTryReplaceAssignmentAccepted,
    AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
    AssignedConsumerTryTakeBatchError, AssignedConsumerTryTakeBatchErrorKind,
    AssignedConsumerTryTakeEventError, AssignedConsumerTryTakeEventErrorKind,
};
pub(crate) use super::assigned_host::{
    AssignedConsumerClosePublisher, AssignedConsumerCompletionNotifier,
    AssignedConsumerCompletionPorts, AssignedConsumerEventPublisher, AssignedConsumerRecvPublisher,
};
pub(in crate::consumer) use super::group::{
    ClassicGroupRevocationAcknowledgeError, GroupConsumerEventPortError,
    GroupConsumerRevocationPortError, GroupConsumerStatePortError,
};
pub(crate) use super::group::{
    GroupConsumerCycleAdmission, GroupConsumerCyclePortErrorCategory, GroupConsumerHostError,
    GroupConsumerPort, GroupConsumerPortDormantReleaseError, GroupConsumerPortRegistrationCategory,
    GroupConsumerRegistry, GroupConsumerShardLockError, GroupConsumerShardOwner,
    GroupConsumerShardWake, GroupConsumerShardWakeError,
};
pub use super::group_acknowledge::{
    GroupConsumerAcknowledgeError, GroupConsumerAcknowledgeErrorKind,
};
pub use super::group_batch::{
    GroupConsumerBatch, GroupConsumerCheckpoint, GroupConsumerCheckpointBuilder,
    GroupConsumerCheckpointMarkError, GroupConsumerCheckpointMarkErrorKind,
    GroupConsumerFetchFailureKind, GroupConsumerHeader, GroupConsumerPositionFailureKind,
    GroupConsumerRecord, GroupConsumerRecords, GroupConsumerTryTakeBatchError,
    GroupConsumerTryTakeBatchErrorKind,
};
pub use super::group_close::{
    GroupConsumerClose, GroupConsumerCloseAdmissionError, GroupConsumerCloseAdmissionErrorKind,
    GroupConsumerCloseError, GroupConsumerCloseErrorKind,
};
pub use super::group_commit::{
    GroupConsumerCommitAccepted, GroupConsumerCommitAdmissionError,
    GroupConsumerCommitAdmissionErrorKind, GroupConsumerCommitBatch,
    GroupConsumerCommitBrokerError, GroupConsumerCommitDeliveryStatus, GroupConsumerCommitFailure,
    GroupConsumerCommitFailureKind, GroupConsumerCommitObserver, GroupConsumerCommitObserverError,
    GroupConsumerCommitOutcome, GroupConsumerCommitPartitionOutcome,
    GroupConsumerCommitPartitionResult,
};
pub use super::group_control::{
    GroupConsumerControl, GroupConsumerControlAccepted, GroupConsumerControlAcceptedFaultKind,
    GroupConsumerControlError, GroupConsumerControlErrorKind, GroupConsumerPartition,
    GroupConsumerPartitionInputError, GroupConsumerPartitionInputErrorKind,
    GroupConsumerResumeCapture, GroupConsumerResumeCaptureError,
    GroupConsumerResumeCaptureErrorKind,
};
pub use super::group_event::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerEvent,
    GroupConsumerMembershipEpoch, GroupConsumerMetadata, GroupConsumerNextEvent,
    GroupConsumerNextEventError, GroupConsumerNextEventErrorKind,
    GroupConsumerRevocationAcknowledgeError, GroupConsumerRevocationAcknowledgeErrorKind,
    GroupConsumerRevocationControl, GroupConsumerState, GroupConsumerStateError,
    GroupConsumerStateErrorKind, GroupConsumerTryTakeEventError,
    GroupConsumerTryTakeEventErrorKind,
};
pub use super::group_recv::{
    GroupConsumerRecv, GroupConsumerRecvError, GroupConsumerRecvErrorKind,
};
pub use super::group_registration::{
    GroupConsumerHandle, GroupConsumerRegistrationError, GroupConsumerRegistrationErrorKind,
};
pub use super::group_registration_request::{
    GroupConsumerClassicAssignor, GroupConsumerMissingOffsetPolicy, GroupConsumerProtocol,
    GroupConsumerRegistration,
};
pub use super::group_release::{
    GroupConsumerDormantReleaseError, GroupConsumerDormantReleaseErrorKind,
};
pub use super::group_seek::{
    GroupConsumerSeek, GroupConsumerSeekAdmissionError, GroupConsumerSeekAdmissionErrorKind,
    GroupConsumerSeekCapture, GroupConsumerSeekError, GroupConsumerSeekErrorKind,
    GroupConsumerSeekPosition,
};
pub use super::group_start::{
    GroupConsumerStartAccepted, GroupConsumerStartCapture, GroupConsumerStartError,
    GroupConsumerStartErrorKind, GroupConsumerStartupFailureKind,
};
pub use super::share::{
    ShareConsumerAssignmentPartition, ShareConsumerClose, ShareConsumerCloseAdmissionError,
    ShareConsumerCloseAdmissionErrorKind, ShareConsumerCloseError, ShareConsumerCloseErrorKind,
    ShareConsumerHandle, ShareConsumerRegistration, ShareConsumerRegistrationError,
    ShareConsumerRegistrationErrorKind, ShareConsumerStartCapture, ShareConsumerState,
    ShareConsumerStateError, ShareConsumerStateErrorKind,
};
pub(crate) use super::share::{
    ShareConsumerPort, ShareConsumerRegistry, ShareConsumerShardLockError, ShareConsumerShardOwner,
    ShareConsumerShardWake, ShareConsumerShardWakeError, ShareMembershipHostError,
    ShareMembershipTurn,
};

pub(crate) use super::{
    assigned_host::{
        AssignedConsumerAdmissionCloser, AssignedConsumerClaimSlot, AssignedConsumerPort,
        AssignedConsumerShardLockError, AssignedConsumerShardOwner, AssignedConsumerShardWake,
        AssignedConsumerShardWakeError, AssignedConsumerShutdownStart,
        build_first_assigned_consumer,
    },
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerFaultKind,
    assigned_owner_model::{AssignedConsumerOwnerBuildError, AssignedConsumerOwnerError},
    assigned_owner_recovery::AssignedConsumerRecoveryReport,
};

#[cfg(test)]
pub(crate) use super::assigned_topics::AssignedPartitionInput;
