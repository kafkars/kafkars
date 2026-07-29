//! Capture-first admission of singular and batched share-group offset listing.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    ListShareGroupOffsetsAdmissionError, ListShareGroupOffsetsAdmissionErrorKind,
    ListShareGroupOffsetsObserver, ListShareGroupOffsetsRequest, ListShareGroupsOffsetsRequest,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating group and topic intent.
    pub fn try_list_share_group_offsets(
        &self,
        request: ListShareGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<ListShareGroupOffsetsAccepted, ListShareGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(admission(
                ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request
            .canonicalize()
            .into_plan()
            .map_err(|_error| admission(ListShareGroupOffsetsAdmissionErrorKind::InvalidRequest))?;
        let now = self.clock.now().map_err(|_error| {
            admission(ListShareGroupOffsetsAdmissionErrorKind::HostUnavailable)
        })?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .list_share_group_offsets
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(ListShareGroupOffsetsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }

    /// Captures one deadline before validating every caller-ordered group query.
    pub fn try_list_share_groups_offsets(
        &self,
        request: ListShareGroupsOffsetsRequest,
        timeout: Duration,
    ) -> Result<ListShareGroupOffsetsAccepted, ListShareGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(admission(
                ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request
            .canonicalize()
            .into_plan()
            .map_err(|_error| admission(ListShareGroupOffsetsAdmissionErrorKind::InvalidRequest))?;
        let now = self.clock.now().map_err(|_error| {
            admission(ListShareGroupOffsetsAdmissionErrorKind::HostUnavailable)
        })?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                ListShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .list_share_group_offsets
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(ListShareGroupOffsetsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(
    kind: ListShareGroupOffsetsAdmissionErrorKind,
) -> ListShareGroupOffsetsAdmissionError {
    ListShareGroupOffsetsAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::ListShareGroupOffsetsHostError,
) -> ListShareGroupOffsetsAcceptedFaultKind {
    match fault {
        super::ListShareGroupOffsetsHostError::Wake => ListShareGroupOffsetsAcceptedFaultKind::Wake,
        _ => ListShareGroupOffsetsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListShareGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted ListShareGroupOffsets work must retain its observer"]
pub struct ListShareGroupOffsetsAccepted {
    observer: ListShareGroupOffsetsObserver,
    fault: Option<ListShareGroupOffsetsAcceptedFaultKind>,
}

impl ListShareGroupOffsetsAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<ListShareGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> ListShareGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for ListShareGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListShareGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
