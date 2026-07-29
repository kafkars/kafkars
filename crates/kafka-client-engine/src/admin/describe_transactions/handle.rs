//! Runtime-neutral admission of one concrete Admin `DescribeTransactions` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminDescribeTransactionsAdmissionError, AdminDescribeTransactionsAdmissionErrorKind,
    AdminDescribeTransactionsObserver, AdminDescribeTransactionsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_transactions(
        &self,
        request: AdminDescribeTransactionsRequest,
        timeout: Duration,
    ) -> Result<AdminDescribeTransactionsAccepted, AdminDescribeTransactionsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminDescribeTransactionsAdmissionError::new(
                    AdminDescribeTransactionsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminDescribeTransactionsAdmissionError::new(
                AdminDescribeTransactionsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AdminDescribeTransactionsAdmissionError::new(
                AdminDescribeTransactionsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_transactions
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminDescribeTransactionsAdmissionError::new)?;
        Ok(AdminDescribeTransactionsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AdminDescribeTransactionsHostError,
) -> AdminDescribeTransactionsAcceptedFaultKind {
    match fault {
        super::AdminDescribeTransactionsHostError::Wake => {
            AdminDescribeTransactionsAcceptedFaultKind::Wake
        }
        _ => AdminDescribeTransactionsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTransactionsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin DescribeTransactions work must retain its observer"]
pub struct AdminDescribeTransactionsAccepted {
    observer: AdminDescribeTransactionsObserver,
    fault: Option<AdminDescribeTransactionsAcceptedFaultKind>,
}

impl AdminDescribeTransactionsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AdminDescribeTransactionsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AdminDescribeTransactionsObserver {
        self.observer
    }
}

impl fmt::Debug for AdminDescribeTransactionsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTransactionsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
