//! Runtime-neutral admission of one concrete Admin `AlterClientQuotas` batch.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AlterClientQuotasAdmissionError, AlterClientQuotasAdmissionErrorKind,
    AlterClientQuotasObserver, AlterClientQuotasRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_alter_client_quotas(
        &self,
        request: AlterClientQuotasRequest,
        timeout: Duration,
    ) -> Result<AlterClientQuotasAccepted, AlterClientQuotasAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AlterClientQuotasAdmissionError::new(
                    AlterClientQuotasAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AlterClientQuotasAdmissionError::new(
                AlterClientQuotasAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AlterClientQuotasAdmissionError::new(
                AlterClientQuotasAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .alter_client_quotas
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AlterClientQuotasAdmissionError::new)?;
        Ok(AlterClientQuotasAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::AlterClientQuotasHostError,
) -> AlterClientQuotasAcceptedFaultKind {
    match fault {
        super::AlterClientQuotasHostError::Wake => AlterClientQuotasAcceptedFaultKind::Wake,
        _ => AlterClientQuotasAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterClientQuotasAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted AlterClientQuotas work must retain its observer"]
pub struct AlterClientQuotasAccepted {
    observer: AlterClientQuotasObserver,
    fault: Option<AlterClientQuotasAcceptedFaultKind>,
}

impl AlterClientQuotasAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AlterClientQuotasAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AlterClientQuotasObserver {
        self.observer
    }
}

impl fmt::Debug for AlterClientQuotasAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterClientQuotasAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
