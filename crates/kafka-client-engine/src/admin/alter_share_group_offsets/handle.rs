//! Capture-first runtime-neutral admission of one share-group offset alteration.

use std::{fmt, time::Duration};

use kafka_client_core::AlterShareGroupOffsetsPlan;

use crate::admin::AdminHandle;

use super::{
    AlterShareGroupOffsetsAdmissionError, AlterShareGroupOffsetsAdmissionErrorKind,
    AlterShareGroupOffsetsObserver, AlterShareGroupOffsetsRequest,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating group and topic intent.
    pub fn try_alter_share_group_offsets(
        &self,
        request: AlterShareGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<AlterShareGroupOffsetsAccepted, AlterShareGroupOffsetsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(AlterShareGroupOffsetsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(admission(
                AlterShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_id, topics) = request.canonicalize().into_parts();
        let plan = AlterShareGroupOffsetsPlan::new(group_id, topics).map_err(|_error| {
            admission(AlterShareGroupOffsetsAdmissionErrorKind::InvalidRequest)
        })?;
        let now = self.clock.now().map_err(|_error| {
            admission(AlterShareGroupOffsetsAdmissionErrorKind::HostUnavailable)
        })?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                AlterShareGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .alter_share_group_offsets
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(AlterShareGroupOffsetsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(
    kind: AlterShareGroupOffsetsAdmissionErrorKind,
) -> AlterShareGroupOffsetsAdmissionError {
    AlterShareGroupOffsetsAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::AlterShareGroupOffsetsHostError,
) -> AlterShareGroupOffsetsAcceptedFaultKind {
    match fault {
        super::AlterShareGroupOffsetsHostError::Wake => {
            AlterShareGroupOffsetsAcceptedFaultKind::Wake
        }
        _ => AlterShareGroupOffsetsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted AlterShareGroupOffsets work must retain its observer"]
pub struct AlterShareGroupOffsetsAccepted {
    observer: AlterShareGroupOffsetsObserver,
    fault: Option<AlterShareGroupOffsetsAcceptedFaultKind>,
}

impl AlterShareGroupOffsetsAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<AlterShareGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> AlterShareGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for AlterShareGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterShareGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
