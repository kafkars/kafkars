//! Capture-first runtime-neutral admission of one configuration-resource listing.

use std::{fmt, time::Duration};

use kafka_client_core::{
    ConfigResourceType as CoreResourceType, ListConfigResourcesPlan as CorePlan,
};

use crate::admin::AdminHandle;

use super::{
    ListConfigResourcesAdmissionError, ListConfigResourcesAdmissionErrorKind,
    ListConfigResourcesObserver,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating resource-type intent.
    pub fn try_list_config_resources(
        &self,
        resource_types: Vec<i8>,
        timeout: Duration,
    ) -> Result<ListConfigResourcesAccepted, ListConfigResourcesAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(ListConfigResourcesAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                ListConfigResourcesAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = plan(resource_types)?;
        let now = self
            .clock
            .now()
            .map_err(|_error| admission(ListConfigResourcesAdmissionErrorKind::HostUnavailable))?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                ListConfigResourcesAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .list_config_resources
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(ListConfigResourcesAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

fn plan(resource_types: Vec<i8>) -> Result<CorePlan, ListConfigResourcesAdmissionError> {
    let mut validated = Vec::new();
    validated
        .try_reserve_exact(resource_types.len())
        .map_err(|_| admission(ListConfigResourcesAdmissionErrorKind::RetainedBytes))?;
    for resource_type in resource_types {
        validated.push(
            CoreResourceType::new(resource_type).map_err(|_error| {
                admission(ListConfigResourcesAdmissionErrorKind::InvalidRequest)
            })?,
        );
    }
    CorePlan::new(validated)
        .map_err(|_error| admission(ListConfigResourcesAdmissionErrorKind::InvalidRequest))
}

const fn admission(
    kind: ListConfigResourcesAdmissionErrorKind,
) -> ListConfigResourcesAdmissionError {
    ListConfigResourcesAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::ListConfigResourcesHostError,
) -> ListConfigResourcesAcceptedFaultKind {
    match fault {
        super::ListConfigResourcesHostError::Wake => ListConfigResourcesAcceptedFaultKind::Wake,
        _ => ListConfigResourcesAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConfigResourcesAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted ListConfigResources work must retain its observer"]
pub struct ListConfigResourcesAccepted {
    observer: ListConfigResourcesObserver,
    fault: Option<ListConfigResourcesAcceptedFaultKind>,
}

impl ListConfigResourcesAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<ListConfigResourcesAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> ListConfigResourcesObserver {
        self.observer
    }
}

impl fmt::Debug for ListConfigResourcesAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConfigResourcesAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
