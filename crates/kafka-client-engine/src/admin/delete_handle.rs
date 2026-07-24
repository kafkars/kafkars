//! Runtime-neutral admission of concrete `DeleteTopics` work.

use std::{fmt, time::Duration};

use super::{
    AdminHandle, DeleteTopicsAdmissionError, DeleteTopicsAdmissionErrorKind, DeleteTopicsObserver,
    DeleteTopicsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded deletion admission at one call boundary.
    pub fn try_delete_topics(
        &self,
        request: DeleteTopicsRequest,
        timeout: Duration,
    ) -> Result<DeleteTopicsAccepted, DeleteTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DeleteTopicsAdmissionError::new(DeleteTopicsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(DeleteTopicsAdmissionError::new(
                DeleteTopicsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            DeleteTopicsAdmissionError::new(DeleteTopicsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            DeleteTopicsAdmissionError::new(DeleteTopicsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .delete_topics
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(DeleteTopicsAdmissionError::new)?;
        Ok(DeleteTopicsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DeleteTopicsHostError,
) -> DeleteTopicsAcceptedFaultKind {
    match fault {
        super::DeleteTopicsHostError::Wake => DeleteTopicsAcceptedFaultKind::Wake,
        _ => DeleteTopicsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteTopicsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DeleteTopics work must retain its observer"]
pub struct DeleteTopicsAccepted {
    observer: DeleteTopicsObserver,
    fault: Option<DeleteTopicsAcceptedFaultKind>,
}

impl DeleteTopicsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DeleteTopicsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DeleteTopicsObserver {
        self.observer
    }
}

impl fmt::Debug for DeleteTopicsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeleteTopicsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
