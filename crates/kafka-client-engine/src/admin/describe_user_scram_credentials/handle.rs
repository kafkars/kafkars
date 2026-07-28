//! Runtime-neutral admission of one concrete Admin `DescribeUserScramCredentials` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeUserScramCredentialsAdmissionError, DescribeUserScramCredentialsAdmissionErrorKind,
    DescribeUserScramCredentialsObserver, DescribeUserScramCredentialsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_user_scram_credentials(
        &self,
        request: DescribeUserScramCredentialsRequest,
        timeout: Duration,
    ) -> Result<DescribeUserScramCredentialsAccepted, DescribeUserScramCredentialsAdmissionError>
    {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeUserScramCredentialsAdmissionError::new(
                    DescribeUserScramCredentialsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeUserScramCredentialsAdmissionError::new(
                DescribeUserScramCredentialsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            DescribeUserScramCredentialsAdmissionError::new(
                DescribeUserScramCredentialsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_user_scram_credentials
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DescribeUserScramCredentialsAdmissionError::new)?;
        Ok(DescribeUserScramCredentialsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeUserScramCredentialsHostError,
) -> DescribeUserScramCredentialsAcceptedFaultKind {
    match fault {
        super::DescribeUserScramCredentialsHostError::Wake => {
            DescribeUserScramCredentialsAcceptedFaultKind::Wake
        }
        _ => DescribeUserScramCredentialsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeUserScramCredentials work must retain its observer"]
pub struct DescribeUserScramCredentialsAccepted {
    observer: DescribeUserScramCredentialsObserver,
    fault: Option<DescribeUserScramCredentialsAcceptedFaultKind>,
}

impl DescribeUserScramCredentialsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeUserScramCredentialsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeUserScramCredentialsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeUserScramCredentialsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeUserScramCredentialsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
