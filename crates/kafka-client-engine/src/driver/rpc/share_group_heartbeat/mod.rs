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

pub(crate) use adapter::{
    ShareGroupHeartbeatCall, ShareGroupHeartbeatCompletionError, ShareGroupHeartbeatResolution,
    ShareGroupHeartbeatRoute,
};
pub(crate) use invalidation::{
    ShareCoordinatorInvalidationAdmissionFailureKind, ShareCoordinatorInvalidationPermission,
    ShareCoordinatorInvalidationPoll, ShareCoordinatorInvalidationTerminalFailure,
    ShareCoordinatorInvalidations,
};
pub(crate) use submission::ShareGroupHeartbeatSubmitErrorKind;
