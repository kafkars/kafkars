//! Bounded ownership of producer records waiting before core admission.
mod attempt;
mod attempt_settlement;
mod attempt_transfer;
mod cell;
mod cell_promotion;
mod entry;
mod error;
mod failure;
mod identity;
mod notification;
mod notification_authority;
mod permit;
mod promotion;
mod recovery;
mod registration;
mod registry;
mod restore;
pub(crate) mod restore_error;
mod state;
mod turn;
pub(crate) mod turn_error;
pub(crate) use attempt::{
    PendingAttemptAcceptFailure, PendingAttemptStateError, PendingPromotionAttempt,
};
pub(crate) use attempt_settlement::PendingAttemptSettleFailure;
pub(crate) use attempt_transfer::{PendingRecordRestoreFailure, PendingRecordTransferState};
pub(crate) use cell::{PendingCellError, PendingCellTransition, PendingSendCell};
pub(crate) use entry::{PendingAdmission, PendingLocalFailure, PendingStartFailure};
pub(crate) use error::{
    PendingAdmissionRejected, PendingAdmissionRejectionReason, PendingRegistryError,
};
pub(crate) use failure::ProducerSendReadyFailure;
pub use failure::{ProducerSendFailure, ProducerSendFailureKind};
pub(crate) use identity::PendingAdmissionId;
pub(crate) use notification::PendingNotificationJob;
pub(crate) use notification_authority::PendingNotificationDispatchAuthority;
pub(crate) use permit::{PendingNotificationPermit, PendingNotificationPermitPool};
#[cfg(test)]
pub(crate) use recovery::PendingNotificationShutdownOwner;
pub(crate) use recovery::{
    PendingNotificationCleanupOwner, PendingNotificationRoute, PendingNotificationRouteMode,
    PendingNotificationRouteProgress, PendingNotificationShutdownFailures,
    PendingPrimaryMissingError, PendingRecoveryJoinError, PendingRecoveryStartupOwner,
};
pub(crate) use registration::PendingSendRegistration;
pub(crate) use registry::{PendingAdmissionRegistry, PendingAdmissionStats};
#[cfg(test)]
pub(crate) use restore_error::PendingAttemptRestoreError;
pub(crate) use restore_error::{PendingAttemptRestoreFailure, PendingAttemptRestoreOutcome};
#[cfg(test)]
mod attempt_settlement_test;
#[cfg(test)]
mod attempt_test;
#[cfg(test)]
mod attempt_transfer_test;
#[cfg(test)]
mod cell_promotion_test;
#[cfg(test)]
mod cell_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod notification_test;
#[cfg(test)]
mod permit_test;
#[cfg(test)]
mod promotion_test;
#[cfg(test)]
mod registration_test;
#[cfg(test)]
mod registry_removal_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod restore_error_test;
#[cfg(test)]
mod restore_test;
#[cfg(test)]
mod state_test;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod turn_error_test;
#[cfg(test)]
mod turn_test;
