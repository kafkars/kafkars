//! Runtime-neutral admission of one concrete Admin `DeleteConsumerGroups` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DeleteConsumerGroupsAdmissionError, DeleteConsumerGroupsAdmissionErrorKind,
    DeleteConsumerGroupsObserver, DeleteConsumerGroupsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_delete_consumer_groups(
        &self,
        request: DeleteConsumerGroupsRequest,
        timeout: Duration,
    ) -> Result<DeleteConsumerGroupsAccepted, DeleteConsumerGroupsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DeleteConsumerGroupsAdmissionError::new(
                    DeleteConsumerGroupsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DeleteConsumerGroupsAdmissionError::new(
                DeleteConsumerGroupsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = request.into_plan().map_err(|_error| {
            DeleteConsumerGroupsAdmissionError::new(
                DeleteConsumerGroupsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .delete_consumer_groups
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DeleteConsumerGroupsAdmissionError::new)?;
        Ok(DeleteConsumerGroupsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DeleteConsumerGroupsHostError,
) -> DeleteConsumerGroupsAcceptedFaultKind {
    match fault {
        super::DeleteConsumerGroupsHostError::Wake => DeleteConsumerGroupsAcceptedFaultKind::Wake,
        super::DeleteConsumerGroupsHostError::Machine(_)
        | super::DeleteConsumerGroupsHostError::Completion(_)
        | super::DeleteConsumerGroupsHostError::UnknownOperation
        | super::DeleteConsumerGroupsHostError::MissingSubmission
        | super::DeleteConsumerGroupsHostError::MissingTerminal
        | super::DeleteConsumerGroupsHostError::SubmissionMismatch
        | super::DeleteConsumerGroupsHostError::InvalidHandoff
        | super::DeleteConsumerGroupsHostError::CallCompletion
        | super::DeleteConsumerGroupsHostError::ByteAccounting
        | super::DeleteConsumerGroupsHostError::Unsettled(_) => {
            DeleteConsumerGroupsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteConsumerGroupsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DeleteConsumerGroups work must retain its observer"]
pub struct DeleteConsumerGroupsAccepted {
    observer: DeleteConsumerGroupsObserver,
    fault: Option<DeleteConsumerGroupsAcceptedFaultKind>,
}

impl DeleteConsumerGroupsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DeleteConsumerGroupsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DeleteConsumerGroupsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteConsumerGroupsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
