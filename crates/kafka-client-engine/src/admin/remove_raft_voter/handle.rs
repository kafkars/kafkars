//! Capture-first runtime-neutral admission of one metadata-quorum voter removal.

use std::{fmt, time::Duration};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    RemoveRaftVoterAdmissionError, RemoveRaftVoterAdmissionErrorKind, RemoveRaftVoterObserver,
    RemoveRaftVoterRequest,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_remove_raft_voter(
        &self,
        timeout: Duration,
    ) -> Result<RemoveRaftVoterCapture<'_>, RemoveRaftVoterAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(RemoveRaftVoterAdmissionErrorKind::InvalidDeadline))?;
        Ok(RemoveRaftVoterCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_remove_raft_voter(
        &self,
        request: RemoveRaftVoterRequest,
        timeout: Duration,
    ) -> Result<RemoveRaftVoterAccepted, RemoveRaftVoterAdmissionError> {
        self.capture_remove_raft_voter(timeout)?.try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting RemoveRaftVoter work"]
pub struct RemoveRaftVoterCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl RemoveRaftVoterCapture<'_> {
    /// Validates bounded intent and atomically reserves terminal ownership.
    pub fn try_submit(
        self,
        request: RemoveRaftVoterRequest,
    ) -> Result<RemoveRaftVoterAccepted, RemoveRaftVoterAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                RemoveRaftVoterAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request
            .into_plan()
            .map_err(|()| admission(RemoveRaftVoterAdmissionErrorKind::InvalidRequest))?;
        let now = self
            .handle
            .clock
            .now()
            .map_err(|_error| admission(RemoveRaftVoterAdmissionErrorKind::HostUnavailable))?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                RemoveRaftVoterAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .handle
            .remove_raft_voter
            .try_admit(now, self.deadline.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(RemoveRaftVoterAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for RemoveRaftVoterCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveRaftVoterCapture")
            .finish_non_exhaustive()
    }
}

const fn admission(kind: RemoveRaftVoterAdmissionErrorKind) -> RemoveRaftVoterAdmissionError {
    RemoveRaftVoterAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::RemoveRaftVoterHostError,
) -> RemoveRaftVoterAcceptedFaultKind {
    match fault {
        super::RemoveRaftVoterHostError::Wake => RemoveRaftVoterAcceptedFaultKind::Wake,
        _ => RemoveRaftVoterAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveRaftVoterAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted RemoveRaftVoter work must retain its observer"]
pub struct RemoveRaftVoterAccepted {
    observer: RemoveRaftVoterObserver,
    fault: Option<RemoveRaftVoterAcceptedFaultKind>,
}

impl RemoveRaftVoterAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<RemoveRaftVoterAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> RemoveRaftVoterObserver {
        self.observer
    }
}

impl fmt::Debug for RemoveRaftVoterAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveRaftVoterAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
