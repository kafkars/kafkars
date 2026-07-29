//! Runtime-neutral admission of one concrete Admin `FenceProducers` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminFenceProducersAdmissionError, AdminFenceProducersAdmissionErrorKind,
    AdminFenceProducersObserver, AdminFenceProducersRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_fence_producers(
        &self,
        request: AdminFenceProducersRequest,
        timeout: Duration,
    ) -> Result<AdminFenceProducersAccepted, AdminFenceProducersAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminFenceProducersAdmissionError::new(
                    AdminFenceProducersAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminFenceProducersAdmissionError::new(
                AdminFenceProducersAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AdminFenceProducersAdmissionError::new(
                AdminFenceProducersAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .fence_producers
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminFenceProducersAdmissionError::new)?;
        Ok(AdminFenceProducersAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AdminFenceProducersHostError,
) -> AdminFenceProducersAcceptedFaultKind {
    match fault {
        super::AdminFenceProducersHostError::Wake => AdminFenceProducersAcceptedFaultKind::Wake,
        _ => AdminFenceProducersAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin FenceProducers work must retain its observer"]
pub struct AdminFenceProducersAccepted {
    observer: AdminFenceProducersObserver,
    fault: Option<AdminFenceProducersAcceptedFaultKind>,
}

impl AdminFenceProducersAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AdminFenceProducersAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AdminFenceProducersObserver {
        self.observer
    }
}

impl fmt::Debug for AdminFenceProducersAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminFenceProducersAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
