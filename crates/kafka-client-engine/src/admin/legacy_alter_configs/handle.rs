//! Capture-first runtime-neutral admission of concrete API 33 resource work.

use std::{fmt, time::Duration};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    LegacyAlterConfigsAdmissionError, LegacyAlterConfigsAdmissionErrorKind,
    LegacyAlterConfigsObserver, LegacyAlterConfigsRequest,
};

impl AdminHandle {
    /// Captures the public deadline before higher-layer request conversion.
    pub fn capture_legacy_alter_configs(
        &self,
        timeout: Duration,
    ) -> Result<LegacyAlterConfigsCapture<'_>, LegacyAlterConfigsAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(LegacyAlterConfigsAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                LegacyAlterConfigsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        Ok(LegacyAlterConfigsCapture {
            handle: self,
            deadline,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_legacy_alter_configs(
        &self,
        request: LegacyAlterConfigsRequest,
        timeout: Duration,
    ) -> Result<LegacyAlterConfigsAccepted, LegacyAlterConfigsAdmissionError> {
        self.capture_legacy_alter_configs(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one admin handle.
#[must_use = "dropping abandons the deadline without admitting legacy configuration work"]
pub struct LegacyAlterConfigsCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
}

impl LegacyAlterConfigsCapture<'_> {
    /// Canonicalizes, validates, and atomically admits one API 33 snapshot.
    pub fn try_submit(
        self,
        request: LegacyAlterConfigsRequest,
    ) -> Result<LegacyAlterConfigsAccepted, LegacyAlterConfigsAdmissionError> {
        let request = request.canonicalize();
        let retention = request
            .retention()
            .ok_or_else(|| admission(LegacyAlterConfigsAdmissionErrorKind::RetainedBytes))?;
        let plan = request
            .into_plan()
            .map_err(|_error| admission(LegacyAlterConfigsAdmissionErrorKind::InvalidRequest))?;
        let now =
            self.handle.clock.now().map_err(|_error| {
                admission(LegacyAlterConfigsAdmissionErrorKind::HostUnavailable)
            })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                LegacyAlterConfigsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .handle
            .legacy_alter_configs
            .try_admit(now, self.deadline.operation_deadline(), plan, retention)
            .map_err(admission)?;
        Ok(LegacyAlterConfigsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for LegacyAlterConfigsCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyAlterConfigsCapture")
            .finish_non_exhaustive()
    }
}

const fn admission(kind: LegacyAlterConfigsAdmissionErrorKind) -> LegacyAlterConfigsAdmissionError {
    LegacyAlterConfigsAdmissionError::new(kind)
}

pub(super) const fn accepted_fault_kind(
    fault: super::LegacyAlterConfigsHostError,
) -> LegacyAlterConfigsAcceptedFaultKind {
    match fault {
        super::LegacyAlterConfigsHostError::Wake => LegacyAlterConfigsAcceptedFaultKind::Wake,
        _ => LegacyAlterConfigsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyAlterConfigsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted LegacyAlterConfigs work must retain its observer"]
pub struct LegacyAlterConfigsAccepted {
    observer: LegacyAlterConfigsObserver,
    fault: Option<LegacyAlterConfigsAcceptedFaultKind>,
}

impl LegacyAlterConfigsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<LegacyAlterConfigsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> LegacyAlterConfigsObserver {
        self.observer
    }
}

impl fmt::Debug for LegacyAlterConfigsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyAlterConfigsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
