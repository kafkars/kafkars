//! Runtime-neutral admission of one concrete Admin `CreateAcls` batch.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    CreateAclsAdmissionError, CreateAclsAdmissionErrorKind, CreateAclsHostError,
    CreateAclsObserver, CreateAclsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_create_acls(
        &self,
        request: CreateAclsRequest,
        timeout: Duration,
    ) -> Result<CreateAclsAccepted, CreateAclsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                CreateAclsAdmissionError::new(CreateAclsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(CreateAclsAdmissionError::new(
                CreateAclsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            CreateAclsAdmissionError::new(CreateAclsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .create_acls
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(CreateAclsAdmissionError::new)?;
        Ok(CreateAclsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(fault: CreateAclsHostError) -> CreateAclsAcceptedFaultKind {
    match fault {
        CreateAclsHostError::Wake => CreateAclsAcceptedFaultKind::Wake,
        _ => CreateAclsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted CreateAcls work must retain its observer"]
pub struct CreateAclsAccepted {
    observer: CreateAclsObserver,
    fault: Option<CreateAclsAcceptedFaultKind>,
}

impl CreateAclsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<CreateAclsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> CreateAclsObserver {
        self.observer
    }
}

impl fmt::Debug for CreateAclsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateAclsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
