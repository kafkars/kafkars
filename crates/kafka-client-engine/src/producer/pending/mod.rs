//! Bounded ownership of producer records waiting before core admission.

mod cell;
mod entry;
mod error;
mod failure;
mod identity;
mod notification;
mod promotion;
mod registration;
mod registry;
mod restore;
mod state;

pub(crate) use cell::{PendingCellError, PendingCellTransition, PendingSendCell};
pub(crate) use entry::{
    PendingAdmission, PendingLocalFailure, PendingLocalFailureKind, PendingRestoreOutcome,
};
pub(crate) use error::{
    PendingAdmissionRejected, PendingAdmissionRejectionReason, PendingRegistryError,
    PendingRestoreFailure,
};
pub use failure::{ProducerSendFailure, ProducerSendFailureKind};
pub(crate) use identity::PendingAdmissionId;
pub(crate) use notification::PendingNotificationJob;
pub(crate) use promotion::PendingPromotion;
#[cfg(test)]
pub(crate) use promotion::PendingPromotionRestore;
pub(crate) use registration::PendingSendRegistration;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "pending host promotion and public send integration follow"
    )
)]
pub(crate) use registry::{PendingAdmissionRegistry, PendingAdmissionStats};

#[cfg(test)]
mod cell_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod notification_test;
#[cfg(test)]
mod promotion_test;
#[cfg(test)]
mod registration_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod restore_test;
#[cfg(test)]
mod state_test;
#[cfg(test)]
pub(crate) mod test_support;
