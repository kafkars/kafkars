//! Capture-first runtime-neutral admission of one broker unregistration.

use std::{fmt, time::Duration};

use kafka_client_core::UnregisterBrokerPlan;

use crate::admin::AdminHandle;

use super::{
    UnregisterBrokerAdmissionError, UnregisterBrokerAdmissionErrorKind, UnregisterBrokerObserver,
};

impl AdminHandle {
    /// Captures one deadline, validates the broker identity, and attempts admission.
    pub fn try_unregister_broker(
        &self,
        broker_id: i32,
        timeout: Duration,
    ) -> Result<UnregisterBrokerAccepted, UnregisterBrokerAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(UnregisterBrokerAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                UnregisterBrokerAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = UnregisterBrokerPlan::new(broker_id)
            .map_err(|_error| admission(UnregisterBrokerAdmissionErrorKind::InvalidRequest))?;
        let admitted = self
            .unregister_broker
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(UnregisterBrokerAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(kind: UnregisterBrokerAdmissionErrorKind) -> UnregisterBrokerAdmissionError {
    UnregisterBrokerAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::UnregisterBrokerHostError,
) -> UnregisterBrokerAcceptedFaultKind {
    match fault {
        super::UnregisterBrokerHostError::Wake => UnregisterBrokerAcceptedFaultKind::Wake,
        _ => UnregisterBrokerAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnregisterBrokerAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted UnregisterBroker work must retain its observer"]
pub struct UnregisterBrokerAccepted {
    observer: UnregisterBrokerObserver,
    fault: Option<UnregisterBrokerAcceptedFaultKind>,
}

impl UnregisterBrokerAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<UnregisterBrokerAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> UnregisterBrokerObserver {
        self.observer
    }
}

impl fmt::Debug for UnregisterBrokerAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnregisterBrokerAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
