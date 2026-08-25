//! Declarative ownership boundary for the first synchronized assigned consumer.

mod assignment;
mod assignment_capture;
#[cfg(test)]
mod assignment_capture_test;
mod assignment_change_capture;
#[cfg(test)]
mod assignment_change_capture_test;
mod assignment_change_error;
#[cfg(test)]
mod assignment_change_error_test;
mod assignment_change_handle;
#[cfg(test)]
mod assignment_change_handle_test;
mod assignment_change_port;
#[cfg(test)]
mod assignment_change_port_test;
mod assignment_change_result;
#[cfg(test)]
mod assignment_change_result_test;
mod assignment_error;
#[cfg(test)]
mod assignment_error_test;
#[cfg(test)]
mod assignment_handle_test;
mod assignment_result;
#[cfg(test)]
mod assignment_result_test;
#[cfg(test)]
mod assignment_test;
mod claim;
#[cfg(test)]
mod claim_test;
mod close_observer;
#[cfg(test)]
mod close_observer_test;
mod completion;
#[cfg(test)]
mod completion_test;
mod control;
mod control_capture;
#[cfg(test)]
mod control_capture_test;
mod control_error;
#[cfg(test)]
mod control_error_test;
#[cfg(test)]
mod control_handle_test;
mod control_result;
#[cfg(test)]
mod control_result_test;
#[cfg(test)]
mod control_test;
mod delivery;
mod event;
mod event_port;
#[cfg(test)]
mod event_port_test;
mod handle;
#[cfg(test)]
mod handle_test;
mod next_event;
mod owner_control;
#[cfg(test)]
mod owner_control_test;
mod port;
#[cfg(test)]
mod port_test;
mod reclaim;
#[cfg(test)]
mod reclaim_test;
mod recv;
mod result;
#[cfg(test)]
mod result_test;
mod shard;
#[cfg(test)]
mod shard_test;
mod start;
#[cfg(test)]
mod start_test;
mod state;
#[cfg(test)]
mod state_test;
mod wake;
#[cfg(test)]
mod wake_test;
pub(crate) use assignment::AssignedPartitionInput;
pub use assignment::{
    AssignedConsumerAssignment, AssignedConsumerAssignmentInputError,
    AssignedConsumerAssignmentInputErrorKind, AssignedConsumerStartPosition,
};
pub use assignment_capture::AssignedConsumerAssignmentCapture;
pub use assignment_change_capture::AssignedConsumerAddAssignmentsCapture;
pub use assignment_change_result::{
    AssignedConsumerTryChangeAssignmentAccepted, AssignedConsumerTryChangeAssignmentError,
    AssignedConsumerTryChangeAssignmentErrorKind,
};
pub use assignment_result::{
    AssignedConsumerAssignmentEpoch, AssignedConsumerTryReplaceAssignmentAccepted,
    AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
};
pub use claim::AssignedConsumerClaimError;
pub(crate) use claim::{AssignedConsumerAdmissionCloser, AssignedConsumerClaimSlot};
pub(crate) use close_observer::AssignedConsumerCloseTerminal;
pub use close_observer::{AssignedConsumerCloseObserver, AssignedConsumerCloseObserverError};
pub(crate) use completion::{
    AssignedConsumerClosePublisher, AssignedConsumerCompletionNotifier,
    AssignedConsumerCompletionPorts, AssignedConsumerEventPublisher, AssignedConsumerRecvPublisher,
};
pub(crate) use control::AssignedConsumerControlInputError;
pub use control::{
    AssignedConsumerPartition, AssignedConsumerPartitionInputError,
    AssignedConsumerPartitionInputErrorKind,
};
pub use control_capture::{AssignedConsumerResumeCapture, AssignedConsumerSeekCapture};
pub use control_result::{
    AssignedConsumerControlAccepted, AssignedConsumerControlError, AssignedConsumerControlErrorKind,
};
pub(crate) use delivery::AssignedConsumerDelivery;
pub use delivery::{
    AssignedConsumerBatch, AssignedConsumerFetchEvidence, AssignedConsumerHeader,
    AssignedConsumerOwnedBatch, AssignedConsumerOwnedRecord, AssignedConsumerOwnedRecords,
    AssignedConsumerRecord, AssignedConsumerRecords, AssignedConsumerTryTakeBatchError,
    AssignedConsumerTryTakeBatchErrorKind,
};
pub use event::{
    AssignedConsumerEvent, AssignedConsumerFetchFailure, AssignedConsumerFetchFailureKind,
    AssignedConsumerFetchFence, AssignedConsumerFetchThrottleFailure,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailure, AssignedConsumerPositionResolutionFailureKind,
    AssignedConsumerTryTakeEventError, AssignedConsumerTryTakeEventErrorKind,
};
pub use handle::AssignedConsumerHandle;
pub use next_event::{
    AssignedConsumerNextEvent, AssignedConsumerNextEventError, AssignedConsumerNextEventErrorKind,
};
pub use recv::{AssignedConsumerRecv, AssignedConsumerRecvError, AssignedConsumerRecvErrorKind};
pub use result::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerTryCloseAccepted,
    AssignedConsumerTryCloseError, AssignedConsumerTryCloseErrorKind,
};
pub(crate) use shard::{
    AssignedConsumerPort, AssignedConsumerShardLockError, AssignedConsumerShardOwner,
    AssignedConsumerShutdownStart,
};
pub(crate) use start::build_first_assigned_consumer;
pub(crate) use wake::{AssignedConsumerShardWake, AssignedConsumerShardWakeError};
