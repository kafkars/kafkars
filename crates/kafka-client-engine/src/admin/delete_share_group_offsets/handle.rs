//! Capture-first runtime-neutral admission of one share-group offset deletion.

use std::{fmt, time::Duration};

use kafka_client_core::DeleteShareGroupOffsetsPlan;

use crate::admin::AdminHandle;

use super::{
    DeleteShareGroupOffsetsAdmissionError, DeleteShareGroupOffsetsAdmissionErrorKind,
    DeleteShareGroupOffsetsObserver, DeleteShareGroupOffsetsRequest,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating group and topic intent.
    pub fn try_delete_share_group_offsets(
        &self,
        request: DeleteShareGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<DeleteShareGroupOffsetsAccepted, DeleteShareGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(DeleteShareGroupOffsetsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(admission(
                DeleteShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_id, topics) = request.canonicalize().into_parts();
        let plan = DeleteShareGroupOffsetsPlan::new(group_id, topics).map_err(|_error| {
            admission(DeleteShareGroupOffsetsAdmissionErrorKind::InvalidRequest)
        })?;
        let now = self.clock.now().map_err(|_error| {
            admission(DeleteShareGroupOffsetsAdmissionErrorKind::HostUnavailable)
        })?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                DeleteShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .delete_share_group_offsets
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(DeleteShareGroupOffsetsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(
    kind: DeleteShareGroupOffsetsAdmissionErrorKind,
) -> DeleteShareGroupOffsetsAdmissionError {
    DeleteShareGroupOffsetsAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::DeleteShareGroupOffsetsHostError,
) -> DeleteShareGroupOffsetsAcceptedFaultKind {
    match fault {
        super::DeleteShareGroupOffsetsHostError::Wake => {
            DeleteShareGroupOffsetsAcceptedFaultKind::Wake
        }
        _ => DeleteShareGroupOffsetsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted DeleteShareGroupOffsets work must retain its observer"]
pub struct DeleteShareGroupOffsetsAccepted {
    observer: DeleteShareGroupOffsetsObserver,
    fault: Option<DeleteShareGroupOffsetsAcceptedFaultKind>,
}

impl DeleteShareGroupOffsetsAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<DeleteShareGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DeleteShareGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteShareGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteShareGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
