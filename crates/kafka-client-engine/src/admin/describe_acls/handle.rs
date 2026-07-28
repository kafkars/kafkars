//! Runtime-neutral admission of one concrete Admin `DescribeAcls` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeAclsAdmissionError, DescribeAclsAdmissionErrorKind, DescribeAclsObserver,
    DescribeAclsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_acls(
        &self,
        request: DescribeAclsRequest,
        timeout: Duration,
    ) -> Result<DescribeAclsAccepted, DescribeAclsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeAclsAdmissionError::new(DescribeAclsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(DescribeAclsAdmissionError::new(
                DescribeAclsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            DescribeAclsAdmissionError::new(DescribeAclsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .describe_acls
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DescribeAclsAdmissionError::new)?;
        Ok(DescribeAclsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeAclsHostError,
) -> DescribeAclsAcceptedFaultKind {
    match fault {
        super::DescribeAclsHostError::Wake => DescribeAclsAcceptedFaultKind::Wake,
        _ => DescribeAclsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeAcls work must retain its observer"]
pub struct DescribeAclsAccepted {
    observer: DescribeAclsObserver,
    fault: Option<DescribeAclsAcceptedFaultKind>,
}

impl DescribeAclsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeAclsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeAclsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeAclsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeAclsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
