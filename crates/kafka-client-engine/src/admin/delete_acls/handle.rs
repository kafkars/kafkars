//! Runtime-neutral admission of one concrete Admin `DeleteAcls` batch.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DeleteAclsAdmissionError, DeleteAclsAdmissionErrorKind, DeleteAclsHostError,
    DeleteAclsObserver, DeleteAclsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_delete_acls(
        &self,
        request: DeleteAclsRequest,
        timeout: Duration,
    ) -> Result<DeleteAclsAccepted, DeleteAclsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DeleteAclsAdmissionError::new(DeleteAclsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(DeleteAclsAdmissionError::new(
                DeleteAclsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            DeleteAclsAdmissionError::new(DeleteAclsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .delete_acls
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DeleteAclsAdmissionError::new)?;
        Ok(DeleteAclsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(fault: DeleteAclsHostError) -> DeleteAclsAcceptedFaultKind {
    match fault {
        DeleteAclsHostError::Wake => DeleteAclsAcceptedFaultKind::Wake,
        _ => DeleteAclsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DeleteAcls work must retain its observer"]
pub struct DeleteAclsAccepted {
    observer: DeleteAclsObserver,
    fault: Option<DeleteAclsAcceptedFaultKind>,
}

impl DeleteAclsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DeleteAclsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DeleteAclsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteAclsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteAclsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
