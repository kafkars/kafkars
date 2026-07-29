//! Capture-first runtime-neutral admission of one committed voter addition.

use std::{fmt, time::Duration};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    AddRaftVoterAdmissionError, AddRaftVoterAdmissionErrorKind, AddRaftVoterObserver,
    AddRaftVoterRequest, model::AddRaftVoterPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_add_raft_voter(
        &self,
        timeout: Duration,
    ) -> Result<AddRaftVoterCapture<'_>, AddRaftVoterAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(AddRaftVoterAdmissionErrorKind::InvalidDeadline))?;
        Ok(AddRaftVoterCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_add_raft_voter(
        &self,
        request: AddRaftVoterRequest,
        timeout: Duration,
    ) -> Result<AddRaftVoterAccepted, AddRaftVoterAdmissionError> {
        self.capture_add_raft_voter(timeout)?.try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting AddRaftVoter work"]
pub struct AddRaftVoterCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl AddRaftVoterCapture<'_> {
    /// Validates bounded intent and atomically reserves terminal ownership.
    pub fn try_submit(
        self,
        request: AddRaftVoterRequest,
    ) -> Result<AddRaftVoterAccepted, AddRaftVoterAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(AddRaftVoterAdmissionErrorKind::InvalidDeadline));
        }
        let plan = request.into_plan().map_err(|error| {
            admission(match error {
                AddRaftVoterPlanFailure::Invalid => AddRaftVoterAdmissionErrorKind::InvalidRequest,
                AddRaftVoterPlanFailure::RetainedBytes => {
                    AddRaftVoterAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let now = self
            .handle
            .clock
            .now()
            .map_err(|_error| admission(AddRaftVoterAdmissionErrorKind::HostUnavailable))?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(AddRaftVoterAdmissionErrorKind::InvalidDeadline));
        }
        let admitted = self
            .handle
            .add_raft_voter
            .try_admit(now, self.deadline.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(AddRaftVoterAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for AddRaftVoterCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AddRaftVoterCapture")
            .finish_non_exhaustive()
    }
}

const fn admission(kind: AddRaftVoterAdmissionErrorKind) -> AddRaftVoterAdmissionError {
    AddRaftVoterAdmissionError::new(kind)
}

const fn accepted_fault_kind(fault: super::AddRaftVoterHostError) -> AddRaftVoterAcceptedFaultKind {
    match fault {
        super::AddRaftVoterHostError::Wake => AddRaftVoterAcceptedFaultKind::Wake,
        _ => AddRaftVoterAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddRaftVoterAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted AddRaftVoter work must retain its observer"]
pub struct AddRaftVoterAccepted {
    observer: AddRaftVoterObserver,
    fault: Option<AddRaftVoterAcceptedFaultKind>,
}

impl AddRaftVoterAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<AddRaftVoterAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> AddRaftVoterObserver {
        self.observer
    }
}

impl fmt::Debug for AddRaftVoterAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AddRaftVoterAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
