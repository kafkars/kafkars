//! Runtime-neutral admission of one concrete Admin `DescribeReplicaLogDirs` query.

use std::{fmt, time::Duration};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    DescribeReplicaLogDirsAdmissionError, DescribeReplicaLogDirsAdmissionErrorKind,
    DescribeReplicaLogDirsObserver, DescribeReplicaLogDirsRequest,
    model::DescribeReplicaLogDirsPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_describe_replica_log_dirs(
        &self,
        timeout: Duration,
    ) -> Result<DescribeReplicaLogDirsCapture<'_>, DescribeReplicaLogDirsAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeReplicaLogDirsAdmissionError::new(
                    DescribeReplicaLogDirsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        Ok(DescribeReplicaLogDirsCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_describe_replica_log_dirs(
        &self,
        request: DescribeReplicaLogDirsRequest,
        timeout: Duration,
    ) -> Result<DescribeReplicaLogDirsAccepted, DescribeReplicaLogDirsAdmissionError> {
        self.capture_describe_replica_log_dirs(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting DescribeReplicaLogDirs work"]
pub struct DescribeReplicaLogDirsCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl DescribeReplicaLogDirsCapture<'_> {
    /// Validates bounded intent and atomically reserves terminal ownership.
    pub fn try_submit(
        self,
        request: DescribeReplicaLogDirsRequest,
    ) -> Result<DescribeReplicaLogDirsAccepted, DescribeReplicaLogDirsAdmissionError> {
        if self.timeout.is_zero() {
            return Err(DescribeReplicaLogDirsAdmissionError::new(
                DescribeReplicaLogDirsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|error| {
            DescribeReplicaLogDirsAdmissionError::new(match error {
                DescribeReplicaLogDirsPlanFailure::Invalid(_) => {
                    DescribeReplicaLogDirsAdmissionErrorKind::InvalidRequest
                }
                DescribeReplicaLogDirsPlanFailure::RetainedBytes => {
                    DescribeReplicaLogDirsAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let now = self.handle.clock.now().map_err(|_error| {
            DescribeReplicaLogDirsAdmissionError::new(
                DescribeReplicaLogDirsAdmissionErrorKind::HostUnavailable,
            )
        })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(DescribeReplicaLogDirsAdmissionError::new(
                DescribeReplicaLogDirsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .handle
            .describe_replica_log_dirs
            .try_admit(now, self.deadline.operation_deadline(), plan)
            .map_err(DescribeReplicaLogDirsAdmissionError::new)?;
        Ok(DescribeReplicaLogDirsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for DescribeReplicaLogDirsCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeReplicaLogDirsCapture")
            .finish_non_exhaustive()
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeReplicaLogDirsHostError,
) -> DescribeReplicaLogDirsAcceptedFaultKind {
    match fault {
        super::DescribeReplicaLogDirsHostError::Wake => {
            DescribeReplicaLogDirsAcceptedFaultKind::Wake
        }
        super::DescribeReplicaLogDirsHostError::Machine(_)
        | super::DescribeReplicaLogDirsHostError::Completion(_)
        | super::DescribeReplicaLogDirsHostError::UnknownOperation
        | super::DescribeReplicaLogDirsHostError::MissingSubmission
        | super::DescribeReplicaLogDirsHostError::MissingReplicas
        | super::DescribeReplicaLogDirsHostError::MissingTerminal
        | super::DescribeReplicaLogDirsHostError::SubmissionMismatch
        | super::DescribeReplicaLogDirsHostError::InvalidHandoff
        | super::DescribeReplicaLogDirsHostError::CallCompletion
        | super::DescribeReplicaLogDirsHostError::ByteAccounting
        | super::DescribeReplicaLogDirsHostError::Unsettled(_) => {
            DescribeReplicaLogDirsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeReplicaLogDirsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DescribeReplicaLogDirs work must retain its observer"]
pub struct DescribeReplicaLogDirsAccepted {
    observer: DescribeReplicaLogDirsObserver,
    fault: Option<DescribeReplicaLogDirsAcceptedFaultKind>,
}

impl DescribeReplicaLogDirsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeReplicaLogDirsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeReplicaLogDirsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeReplicaLogDirsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeReplicaLogDirsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
