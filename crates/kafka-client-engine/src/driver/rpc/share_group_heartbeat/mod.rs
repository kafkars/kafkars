//! Closed tracked-call adapter for `ShareGroupHeartbeat` v1.

mod adapter;
mod invalidation;
mod invalidation_drive;
#[cfg(test)]
mod invalidation_drive_test;
#[cfg(test)]
mod invalidation_test;
mod submission;
#[cfg(test)]
mod submission_test;

#[expect(
    unused_imports,
    reason = "the hosted share membership owner lands in the next checkpoint"
)]
pub(crate) use adapter::{
    ShareGroupHeartbeatCall, ShareGroupHeartbeatCompletionError, ShareGroupHeartbeatResolution,
    ShareGroupHeartbeatRoute,
};
#[expect(
    unused_imports,
    reason = "the hosted share membership registry lands in the next checkpoint"
)]
pub(crate) use invalidation::{
    PendingShareCoordinatorInvalidation, ShareCoordinatorInvalidationAdmissionFailureKind,
    ShareCoordinatorInvalidationPermission, ShareCoordinatorInvalidationPoll,
    ShareCoordinatorInvalidationReserveError, ShareCoordinatorInvalidationTerminalFailure,
    ShareCoordinatorInvalidations,
};
#[expect(
    unused_imports,
    reason = "the hosted share membership owner lands in the next checkpoint"
)]
pub(crate) use submission::ShareGroupHeartbeatSubmitErrorKind;
