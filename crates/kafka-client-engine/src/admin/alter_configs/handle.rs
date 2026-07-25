//! Runtime-neutral admission of concrete topic `IncrementalAlterConfigs` work.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    IncrementalAlterConfigsAdmissionError, IncrementalAlterConfigsAdmissionErrorKind,
    IncrementalAlterConfigsObserver, IncrementalAlterConfigsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_incremental_alter_configs(
        &self,
        request: IncrementalAlterConfigsRequest,
        timeout: Duration,
    ) -> Result<IncrementalAlterConfigsAccepted, IncrementalAlterConfigsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                IncrementalAlterConfigsAdmissionError::new(
                    IncrementalAlterConfigsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(IncrementalAlterConfigsAdmissionError::new(
                IncrementalAlterConfigsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retention = request.retention().ok_or_else(|| {
            IncrementalAlterConfigsAdmissionError::new(
                IncrementalAlterConfigsAdmissionErrorKind::RetainedBytes,
            )
        })?;
        let plan = request.into_plan().map_err(|_error| {
            IncrementalAlterConfigsAdmissionError::new(
                IncrementalAlterConfigsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .incremental_alter_configs
            .try_admit(capture.now(), capture.operation_deadline(), plan, retention)
            .map_err(IncrementalAlterConfigsAdmissionError::new)?;
        Ok(IncrementalAlterConfigsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::IncrementalAlterConfigsHostError,
) -> IncrementalAlterConfigsAcceptedFaultKind {
    match fault {
        super::IncrementalAlterConfigsHostError::Wake => {
            IncrementalAlterConfigsAcceptedFaultKind::Wake
        }
        _ => IncrementalAlterConfigsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalAlterConfigsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted IncrementalAlterConfigs work must retain its observer"]
pub struct IncrementalAlterConfigsAccepted {
    observer: IncrementalAlterConfigsObserver,
    fault: Option<IncrementalAlterConfigsAcceptedFaultKind>,
}

impl IncrementalAlterConfigsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<IncrementalAlterConfigsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> IncrementalAlterConfigsObserver {
        self.observer
    }
}

impl fmt::Debug for IncrementalAlterConfigsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IncrementalAlterConfigsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
