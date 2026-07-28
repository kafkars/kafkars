//! Runtime-neutral admission of one concrete Admin `ListOffsets` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminListOffsetsAdmissionError, AdminListOffsetsAdmissionErrorKind, AdminListOffsetsObserver,
    AdminListOffsetsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_list_offsets(
        &self,
        request: AdminListOffsetsRequest,
        timeout: Duration,
    ) -> Result<AdminListOffsetsAccepted, AdminListOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminListOffsetsAdmissionError::new(
                    AdminListOffsetsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminListOffsetsAdmissionError::new(
                AdminListOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = request.into_plan().map_err(|_error| {
            AdminListOffsetsAdmissionError::new(AdminListOffsetsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .list_offsets
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminListOffsetsAdmissionError::new)?;
        Ok(AdminListOffsetsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::AdminListOffsetsHostError,
) -> AdminListOffsetsAcceptedFaultKind {
    match fault {
        super::AdminListOffsetsHostError::Wake => AdminListOffsetsAcceptedFaultKind::Wake,
        super::AdminListOffsetsHostError::Machine(_)
        | super::AdminListOffsetsHostError::Completion(_)
        | super::AdminListOffsetsHostError::UnknownOperation
        | super::AdminListOffsetsHostError::MissingSubmission
        | super::AdminListOffsetsHostError::MissingTerminal
        | super::AdminListOffsetsHostError::SubmissionMismatch
        | super::AdminListOffsetsHostError::InvalidHandoff
        | super::AdminListOffsetsHostError::CallCompletion
        | super::AdminListOffsetsHostError::ByteAccounting
        | super::AdminListOffsetsHostError::Unsettled(_) => {
            AdminListOffsetsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin ListOffsets work must retain its observer"]
pub struct AdminListOffsetsAccepted {
    observer: AdminListOffsetsObserver,
    fault: Option<AdminListOffsetsAcceptedFaultKind>,
}

impl AdminListOffsetsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AdminListOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AdminListOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for AdminListOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
