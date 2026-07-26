//! Runtime-neutral admission of one concrete offset-deletion request.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DeleteConsumerGroupOffsetsAdmissionError, DeleteConsumerGroupOffsetsAdmissionErrorKind,
    DeleteConsumerGroupOffsetsObserver, DeleteConsumerGroupOffsetsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_delete_consumer_group_offsets(
        &self,
        request: DeleteConsumerGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<DeleteConsumerGroupOffsetsAccepted, DeleteConsumerGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DeleteConsumerGroupOffsetsAdmissionError::new(
                    DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DeleteConsumerGroupOffsetsAdmissionError::new(
                DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = request.into_plan().map_err(|_error| {
            DeleteConsumerGroupOffsetsAdmissionError::new(
                DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .delete_consumer_group_offsets
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DeleteConsumerGroupOffsetsAdmissionError::new)?;
        Ok(DeleteConsumerGroupOffsetsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DeleteConsumerGroupOffsetsHostError,
) -> DeleteConsumerGroupOffsetsAcceptedFaultKind {
    match fault {
        super::DeleteConsumerGroupOffsetsHostError::Wake => {
            DeleteConsumerGroupOffsetsAcceptedFaultKind::Wake
        }
        super::DeleteConsumerGroupOffsetsHostError::Machine(_)
        | super::DeleteConsumerGroupOffsetsHostError::Completion(_)
        | super::DeleteConsumerGroupOffsetsHostError::UnknownOperation
        | super::DeleteConsumerGroupOffsetsHostError::MissingSubmission
        | super::DeleteConsumerGroupOffsetsHostError::MissingTerminal
        | super::DeleteConsumerGroupOffsetsHostError::SubmissionMismatch
        | super::DeleteConsumerGroupOffsetsHostError::InvalidHandoff
        | super::DeleteConsumerGroupOffsetsHostError::CallCompletion
        | super::DeleteConsumerGroupOffsetsHostError::ByteAccounting
        | super::DeleteConsumerGroupOffsetsHostError::Unsettled(_) => {
            DeleteConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DeleteConsumerGroupOffsets work must retain its observer"]
pub struct DeleteConsumerGroupOffsetsAccepted {
    observer: DeleteConsumerGroupOffsetsObserver,
    fault: Option<DeleteConsumerGroupOffsetsAcceptedFaultKind>,
}

impl DeleteConsumerGroupOffsetsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DeleteConsumerGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DeleteConsumerGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteConsumerGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
