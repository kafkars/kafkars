//! Declarative ownership boundary for the first synchronized assigned consumer.

mod assignment;
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
mod event_port;
#[cfg(test)]
mod event_port_test;
mod handle;
#[cfg(test)]
mod handle_test;
mod port;
#[cfg(test)]
mod port_test;
mod reclaim;
#[cfg(test)]
mod reclaim_test;
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
pub use assignment_result::{
    AssignedConsumerAssignmentEpoch, AssignedConsumerTryReplaceAssignmentAccepted,
    AssignedConsumerTryReplaceAssignmentError, AssignedConsumerTryReplaceAssignmentErrorKind,
};
pub use claim::AssignedConsumerClaimError;
pub(crate) use claim::{AssignedConsumerAdmissionCloser, AssignedConsumerClaimSlot};
pub(crate) use close_observer::AssignedConsumerCloseTerminal;
pub use close_observer::{AssignedConsumerCloseObserver, AssignedConsumerCloseObserverError};
pub(crate) use completion::{AssignedConsumerClosePublisher, AssignedConsumerCompletionNotifier};
pub use handle::AssignedConsumerHandle;
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
