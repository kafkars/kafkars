//! Capture-first runtime-neutral admission of one finalized-feature mutation.

use std::{fmt, time::Duration};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    UpdateFeaturesAdmissionError, UpdateFeaturesAdmissionErrorKind, UpdateFeaturesObserver,
    UpdateFeaturesRequest, model::UpdateFeaturesPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_update_features(
        &self,
        timeout: Duration,
    ) -> Result<UpdateFeaturesCapture<'_>, UpdateFeaturesAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(UpdateFeaturesAdmissionErrorKind::InvalidDeadline))?;
        Ok(UpdateFeaturesCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_update_features(
        &self,
        request: UpdateFeaturesRequest,
        timeout: Duration,
    ) -> Result<UpdateFeaturesAccepted, UpdateFeaturesAdmissionError> {
        self.capture_update_features(timeout)?.try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting UpdateFeatures work"]
pub struct UpdateFeaturesCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl UpdateFeaturesCapture<'_> {
    /// Validates bounded intent and atomically reserves terminal ownership.
    pub fn try_submit(
        self,
        request: UpdateFeaturesRequest,
    ) -> Result<UpdateFeaturesAccepted, UpdateFeaturesAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(UpdateFeaturesAdmissionErrorKind::InvalidDeadline));
        }
        let plan = request.plan().map_err(|error| {
            admission(match error {
                UpdateFeaturesPlanFailure::Invalid => {
                    UpdateFeaturesAdmissionErrorKind::InvalidRequest
                }
                UpdateFeaturesPlanFailure::RetainedBytes => {
                    UpdateFeaturesAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        drop(request);

        let now = self
            .handle
            .clock
            .now()
            .map_err(|_error| admission(UpdateFeaturesAdmissionErrorKind::HostUnavailable))?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(UpdateFeaturesAdmissionErrorKind::InvalidDeadline));
        }
        let admitted = self
            .handle
            .update_features
            .try_admit(now, self.deadline.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(UpdateFeaturesAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for UpdateFeaturesCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateFeaturesCapture")
            .finish_non_exhaustive()
    }
}

const fn admission(kind: UpdateFeaturesAdmissionErrorKind) -> UpdateFeaturesAdmissionError {
    UpdateFeaturesAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::UpdateFeaturesHostError,
) -> UpdateFeaturesAcceptedFaultKind {
    match fault {
        super::UpdateFeaturesHostError::Wake => UpdateFeaturesAcceptedFaultKind::Wake,
        _ => UpdateFeaturesAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateFeaturesAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted UpdateFeatures work must retain its observer"]
pub struct UpdateFeaturesAccepted {
    observer: UpdateFeaturesObserver,
    fault: Option<UpdateFeaturesAcceptedFaultKind>,
}

impl UpdateFeaturesAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<UpdateFeaturesAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> UpdateFeaturesObserver {
        self.observer
    }
}

impl fmt::Debug for UpdateFeaturesAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateFeaturesAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
