//! Runtime-neutral admission of one Admin client-metrics resource listing.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    ListClientMetricsResourcesAdmissionError, ListClientMetricsResourcesAdmissionErrorKind,
    ListClientMetricsResourcesObserver,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_list_client_metrics_resources(
        &self,
        timeout: Duration,
    ) -> Result<ListClientMetricsResourcesAccepted, ListClientMetricsResourcesAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                ListClientMetricsResourcesAdmissionError::new(
                    ListClientMetricsResourcesAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(ListClientMetricsResourcesAdmissionError::new(
                ListClientMetricsResourcesAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .list_client_metrics_resources
            .try_admit(capture.now(), capture.operation_deadline())
            .map_err(ListClientMetricsResourcesAdmissionError::new)?;
        Ok(ListClientMetricsResourcesAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::ListClientMetricsResourcesHostError,
) -> ListClientMetricsResourcesAcceptedFaultKind {
    match fault {
        super::ListClientMetricsResourcesHostError::Wake => {
            ListClientMetricsResourcesAcceptedFaultKind::Wake
        }
        _ => ListClientMetricsResourcesAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListClientMetricsResourcesAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted ListClientMetricsResources work must retain its observer"]
pub struct ListClientMetricsResourcesAccepted {
    observer: ListClientMetricsResourcesObserver,
    fault: Option<ListClientMetricsResourcesAcceptedFaultKind>,
}

impl ListClientMetricsResourcesAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<ListClientMetricsResourcesAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> ListClientMetricsResourcesObserver {
        self.observer
    }
}

impl fmt::Debug for ListClientMetricsResourcesAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListClientMetricsResourcesAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
