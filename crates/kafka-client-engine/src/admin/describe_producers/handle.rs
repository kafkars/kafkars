//! Runtime-neutral admission of one concrete Admin `DescribeProducers` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminDescribeProducersAdmissionError, AdminDescribeProducersAdmissionErrorKind,
    AdminDescribeProducersObserver, AdminDescribeProducersRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_producers(
        &self,
        request: AdminDescribeProducersRequest,
        timeout: Duration,
    ) -> Result<AdminDescribeProducersAccepted, AdminDescribeProducersAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminDescribeProducersAdmissionError::new(
                    AdminDescribeProducersAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminDescribeProducersAdmissionError::new(
                AdminDescribeProducersAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AdminDescribeProducersAdmissionError::new(
                AdminDescribeProducersAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_producers
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminDescribeProducersAdmissionError::new)?;
        Ok(AdminDescribeProducersAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AdminDescribeProducersHostError,
) -> AdminDescribeProducersAcceptedFaultKind {
    match fault {
        super::AdminDescribeProducersHostError::Wake => {
            AdminDescribeProducersAcceptedFaultKind::Wake
        }
        _ => AdminDescribeProducersAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeProducersAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DescribeProducers work must retain its observer"]
pub struct AdminDescribeProducersAccepted {
    observer: AdminDescribeProducersObserver,
    fault: Option<AdminDescribeProducersAcceptedFaultKind>,
}

impl AdminDescribeProducersAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AdminDescribeProducersAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AdminDescribeProducersObserver {
        self.observer
    }
}

impl fmt::Debug for AdminDescribeProducersAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeProducersAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
