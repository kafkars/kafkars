//! Runtime-neutral admission of one concrete Admin `DescribeClientQuotas` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeClientQuotasAdmissionError, DescribeClientQuotasAdmissionErrorKind,
    DescribeClientQuotasObserver, DescribeClientQuotasRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_client_quotas(
        &self,
        request: DescribeClientQuotasRequest,
        timeout: Duration,
    ) -> Result<DescribeClientQuotasAccepted, DescribeClientQuotasAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeClientQuotasAdmissionError::new(
                    DescribeClientQuotasAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeClientQuotasAdmissionError::new(
                DescribeClientQuotasAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            DescribeClientQuotasAdmissionError::new(
                DescribeClientQuotasAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_client_quotas
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DescribeClientQuotasAdmissionError::new)?;
        Ok(DescribeClientQuotasAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeClientQuotasHostError,
) -> DescribeClientQuotasAcceptedFaultKind {
    match fault {
        super::DescribeClientQuotasHostError::Wake => DescribeClientQuotasAcceptedFaultKind::Wake,
        _ => DescribeClientQuotasAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotasAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeClientQuotas work must retain its observer"]
pub struct DescribeClientQuotasAccepted {
    observer: DescribeClientQuotasObserver,
    fault: Option<DescribeClientQuotasAcceptedFaultKind>,
}

impl DescribeClientQuotasAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeClientQuotasAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeClientQuotasObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeClientQuotasAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeClientQuotasAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
