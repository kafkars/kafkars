//! Runtime-neutral admission of one concrete Admin `DeleteRecords` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DeleteRecordsAdmissionError, DeleteRecordsAdmissionErrorKind, DeleteRecordsObserver,
    DeleteRecordsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_delete_records(
        &self,
        request: DeleteRecordsRequest,
        timeout: Duration,
    ) -> Result<DeleteRecordsAccepted, DeleteRecordsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DeleteRecordsAdmissionError::new(DeleteRecordsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(DeleteRecordsAdmissionError::new(
                DeleteRecordsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = request.into_plan().map_err(|_error| {
            DeleteRecordsAdmissionError::new(DeleteRecordsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .delete_records
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DeleteRecordsAdmissionError::new)?;
        Ok(DeleteRecordsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DeleteRecordsHostError,
) -> DeleteRecordsAcceptedFaultKind {
    match fault {
        super::DeleteRecordsHostError::Wake => DeleteRecordsAcceptedFaultKind::Wake,
        super::DeleteRecordsHostError::Machine(_)
        | super::DeleteRecordsHostError::Completion(_)
        | super::DeleteRecordsHostError::UnknownOperation
        | super::DeleteRecordsHostError::MissingSubmission
        | super::DeleteRecordsHostError::MissingTerminal
        | super::DeleteRecordsHostError::SubmissionMismatch
        | super::DeleteRecordsHostError::InvalidHandoff
        | super::DeleteRecordsHostError::CallCompletion
        | super::DeleteRecordsHostError::ByteAccounting
        | super::DeleteRecordsHostError::Unsettled(_) => {
            DeleteRecordsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteRecordsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DeleteRecords work must retain its observer"]
pub struct DeleteRecordsAccepted {
    observer: DeleteRecordsObserver,
    fault: Option<DeleteRecordsAcceptedFaultKind>,
}

impl DeleteRecordsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DeleteRecordsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DeleteRecordsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteRecordsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteRecordsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
