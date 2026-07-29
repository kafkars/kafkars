//! Runtime-neutral admission of one concrete Admin `ListTransactions` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminListTransactionsAdmissionError, AdminListTransactionsAdmissionErrorKind,
    AdminListTransactionsObserver, AdminListTransactionsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_list_transactions(
        &self,
        request: AdminListTransactionsRequest,
        timeout: Duration,
    ) -> Result<AdminListTransactionsAccepted, AdminListTransactionsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminListTransactionsAdmissionError::new(
                    AdminListTransactionsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminListTransactionsAdmissionError::new(
                AdminListTransactionsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AdminListTransactionsAdmissionError::new(
                AdminListTransactionsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .list_transactions
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminListTransactionsAdmissionError::new)?;
        Ok(AdminListTransactionsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AdminListTransactionsHostError,
) -> AdminListTransactionsAcceptedFaultKind {
    match fault {
        super::AdminListTransactionsHostError::Wake => AdminListTransactionsAcceptedFaultKind::Wake,
        _ => AdminListTransactionsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListTransactionsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin ListTransactions work must retain its observer"]
pub struct AdminListTransactionsAccepted {
    observer: AdminListTransactionsObserver,
    fault: Option<AdminListTransactionsAcceptedFaultKind>,
}

impl AdminListTransactionsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AdminListTransactionsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> AdminListTransactionsObserver {
        self.observer
    }
}

impl fmt::Debug for AdminListTransactionsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminListTransactionsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
