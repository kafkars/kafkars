//! Runtime-neutral admission of one concrete consumer-group offset query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    ListConsumerGroupOffsetsAdmissionError, ListConsumerGroupOffsetsAdmissionErrorKind,
    ListConsumerGroupOffsetsObserver, ListConsumerGroupOffsetsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_list_consumer_group_offsets(
        &self,
        request: ListConsumerGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<ListConsumerGroupOffsetsAccepted, ListConsumerGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                ListConsumerGroupOffsetsAdmissionError::new(
                    ListConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(ListConsumerGroupOffsetsAdmissionError::new(
                ListConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = request.into_plan().map_err(|_error| {
            ListConsumerGroupOffsetsAdmissionError::new(
                ListConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .list_consumer_group_offsets
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(ListConsumerGroupOffsetsAdmissionError::new)?;
        Ok(ListConsumerGroupOffsetsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::ListConsumerGroupOffsetsHostError,
) -> ListConsumerGroupOffsetsAcceptedFaultKind {
    match fault {
        super::ListConsumerGroupOffsetsHostError::Wake => {
            ListConsumerGroupOffsetsAcceptedFaultKind::Wake
        }
        super::ListConsumerGroupOffsetsHostError::Machine(_)
        | super::ListConsumerGroupOffsetsHostError::Completion(_)
        | super::ListConsumerGroupOffsetsHostError::UnknownOperation
        | super::ListConsumerGroupOffsetsHostError::MissingSubmission
        | super::ListConsumerGroupOffsetsHostError::MissingTerminal
        | super::ListConsumerGroupOffsetsHostError::SubmissionMismatch
        | super::ListConsumerGroupOffsetsHostError::InvalidHandoff
        | super::ListConsumerGroupOffsetsHostError::CallCompletion
        | super::ListConsumerGroupOffsetsHostError::ByteAccounting
        | super::ListConsumerGroupOffsetsHostError::Unsettled(_) => {
            ListConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted ListConsumerGroupOffsets work must retain its observer"]
pub struct ListConsumerGroupOffsetsAccepted {
    observer: ListConsumerGroupOffsetsObserver,
    fault: Option<ListConsumerGroupOffsetsAcceptedFaultKind>,
}

impl ListConsumerGroupOffsetsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<ListConsumerGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> ListConsumerGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for ListConsumerGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
