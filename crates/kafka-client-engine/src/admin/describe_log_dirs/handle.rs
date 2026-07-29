//! Runtime-neutral admission of one concrete Admin `DescribeLogDirs` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeLogDirsAdmissionError, DescribeLogDirsAdmissionErrorKind, DescribeLogDirsObserver,
    DescribeLogDirsPlanFailure, DescribeLogDirsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_log_dirs(
        &self,
        request: DescribeLogDirsRequest,
        timeout: Duration,
    ) -> Result<DescribeLogDirsAccepted, DescribeLogDirsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeLogDirsAdmissionError::new(
                    DescribeLogDirsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeLogDirsAdmissionError::new(
                DescribeLogDirsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request
            .canonicalize()
            .into_plan()
            .map_err(|error| match error {
                DescribeLogDirsPlanFailure::Invalid(_error) => DescribeLogDirsAdmissionError::new(
                    DescribeLogDirsAdmissionErrorKind::InvalidRequest,
                ),
                DescribeLogDirsPlanFailure::RetainedBytes => DescribeLogDirsAdmissionError::new(
                    DescribeLogDirsAdmissionErrorKind::RetainedBytes,
                ),
            })?;
        let admission = self
            .describe_log_dirs
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DescribeLogDirsAdmissionError::new)?;
        Ok(DescribeLogDirsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeLogDirsHostError,
) -> DescribeLogDirsAcceptedFaultKind {
    match fault {
        super::DescribeLogDirsHostError::Wake => DescribeLogDirsAcceptedFaultKind::Wake,
        super::DescribeLogDirsHostError::Machine(_)
        | super::DescribeLogDirsHostError::Completion(_)
        | super::DescribeLogDirsHostError::UnknownOperation
        | super::DescribeLogDirsHostError::MissingSubmission
        | super::DescribeLogDirsHostError::MissingTerminal
        | super::DescribeLogDirsHostError::SubmissionMismatch
        | super::DescribeLogDirsHostError::InvalidHandoff
        | super::DescribeLogDirsHostError::CallCompletion
        | super::DescribeLogDirsHostError::ByteAccounting
        | super::DescribeLogDirsHostError::Unsettled(_) => {
            DescribeLogDirsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeLogDirsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DescribeLogDirs work must retain its observer"]
pub struct DescribeLogDirsAccepted {
    observer: DescribeLogDirsObserver,
    fault: Option<DescribeLogDirsAcceptedFaultKind>,
}

impl DescribeLogDirsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeLogDirsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeLogDirsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeLogDirsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeLogDirsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
