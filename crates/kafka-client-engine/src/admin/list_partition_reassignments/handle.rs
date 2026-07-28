//! Runtime-neutral admission of one concrete reassignment-listing query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    ListPartitionReassignmentsAdmissionError, ListPartitionReassignmentsAdmissionErrorKind,
    ListPartitionReassignmentsObserver, ListPartitionReassignmentsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_list_partition_reassignments(
        &self,
        request: ListPartitionReassignmentsRequest,
        timeout: Duration,
    ) -> Result<ListPartitionReassignmentsAccepted, ListPartitionReassignmentsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                ListPartitionReassignmentsAdmissionError::new(
                    ListPartitionReassignmentsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(ListPartitionReassignmentsAdmissionError::new(
                ListPartitionReassignmentsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            ListPartitionReassignmentsAdmissionError::new(
                ListPartitionReassignmentsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .list_partition_reassignments
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(ListPartitionReassignmentsAdmissionError::new)?;
        Ok(ListPartitionReassignmentsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::ListPartitionReassignmentsHostError,
) -> ListPartitionReassignmentsAcceptedFaultKind {
    match fault {
        super::ListPartitionReassignmentsHostError::Wake => {
            ListPartitionReassignmentsAcceptedFaultKind::Wake
        }
        _ => ListPartitionReassignmentsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListPartitionReassignmentsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted ListPartitionReassignments work must retain its observer"]
pub struct ListPartitionReassignmentsAccepted {
    observer: ListPartitionReassignmentsObserver,
    fault: Option<ListPartitionReassignmentsAcceptedFaultKind>,
}

impl ListPartitionReassignmentsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<ListPartitionReassignmentsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> ListPartitionReassignmentsObserver {
        self.observer
    }
}

impl fmt::Debug for ListPartitionReassignmentsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListPartitionReassignmentsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
