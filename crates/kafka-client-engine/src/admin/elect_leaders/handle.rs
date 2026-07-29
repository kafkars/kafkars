//! Runtime-neutral admission at the election public call boundary.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::model::ElectLeadersPlanFailure;
use super::{
    ElectLeadersAdmissionError, ElectLeadersAdmissionErrorKind, ElectLeadersObserver,
    ElectLeadersRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission and captures the sole deadline.
    pub fn try_elect_leaders(
        &self,
        request: ElectLeadersRequest,
        timeout: Duration,
    ) -> Result<ElectLeadersAccepted, ElectLeadersAdmissionError> {
        let capture = match self.clock.capture_deadline_after(timeout) {
            Ok(capture) => capture,
            Err(_error) => {
                return Err(ElectLeadersAdmissionError::new(
                    ElectLeadersAdmissionErrorKind::InvalidDeadline,
                    request,
                ));
            }
        };
        if timeout.is_zero() {
            return Err(ElectLeadersAdmissionError::new(
                ElectLeadersAdmissionErrorKind::InvalidDeadline,
                request,
            ));
        }
        if request
            .preparation_charge()
            .is_none_or(|charge| charge > super::host::ELECT_LEADERS_RETAINED_BYTES)
        {
            return Err(ElectLeadersAdmissionError::new(
                ElectLeadersAdmissionErrorKind::RetainedBytes,
                request,
            ));
        }
        let plan = request
            .clone()
            .canonicalize()
            .into_plan()
            .map_err(|error| {
                let kind = match error {
                    ElectLeadersPlanFailure::Invalid(_error) => {
                        ElectLeadersAdmissionErrorKind::InvalidRequest
                    }
                    ElectLeadersPlanFailure::RetainedBytes => {
                        ElectLeadersAdmissionErrorKind::RetainedBytes
                    }
                };
                ElectLeadersAdmissionError::new(kind, request.clone())
            })?;
        let admission = self
            .elect_leaders
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(|kind| ElectLeadersAdmissionError::new(kind, request))?;
        Ok(ElectLeadersAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(fault: super::ElectLeadersHostError) -> ElectLeadersAcceptedFaultKind {
    match fault {
        super::ElectLeadersHostError::Wake => ElectLeadersAcceptedFaultKind::Wake,
        _ => ElectLeadersAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElectLeadersAcceptedFaultKind {
    /// Reactor notification failed after ownership committed.
    Wake,
    /// The retained host reported an invariant failure after admission.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted ElectLeaders work must retain its observer"]
pub struct ElectLeadersAccepted {
    observer: ElectLeadersObserver,
    fault: Option<ElectLeadersAcceptedFaultKind>,
}

impl ElectLeadersAccepted {
    /// Returns any post-commit degradation observed during admission.
    pub const fn fault(&self) -> Option<ElectLeadersAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the accepted value into its single terminal observer.
    pub fn into_observer(self) -> ElectLeadersObserver {
        self.observer
    }
}

impl fmt::Debug for ElectLeadersAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ElectLeadersAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
