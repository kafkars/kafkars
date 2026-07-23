//! Bounded ownership of producer records waiting before core admission.

mod entry;
mod error;
mod identity;
mod registry;

pub(crate) use entry::{PendingAdmission, PendingLocalFailure, PendingLocalFailureKind};
pub(crate) use error::{
    PendingAdmissionRejected, PendingAdmissionRejectionReason, PendingRegistryError,
};
pub(crate) use identity::PendingAdmissionId;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "pending host promotion and public send integration follow"
    )
)]
pub(crate) use registry::{PendingAdmissionRegistry, PendingAdmissionStats};

#[cfg(test)]
mod registry_test;
