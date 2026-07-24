//! Runtime-neutral admission of concrete `CreatePartitions` work.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    CreatePartitionsAdmissionError, CreatePartitionsAdmissionErrorKind, CreatePartitionsObserver,
    CreatePartitionsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_create_partitions(
        &self,
        request: CreatePartitionsRequest,
        timeout: Duration,
    ) -> Result<CreatePartitionsAccepted, CreatePartitionsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                CreatePartitionsAdmissionError::new(
                    CreatePartitionsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(CreatePartitionsAdmissionError::new(
                CreatePartitionsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            CreatePartitionsAdmissionError::new(CreatePartitionsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            CreatePartitionsAdmissionError::new(CreatePartitionsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .create_partitions
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(CreatePartitionsAdmissionError::new)?;
        Ok(CreatePartitionsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::CreatePartitionsHostError,
) -> CreatePartitionsAcceptedFaultKind {
    match fault {
        super::CreatePartitionsHostError::Wake => CreatePartitionsAcceptedFaultKind::Wake,
        _ => CreatePartitionsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePartitionsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted CreatePartitions work must retain its observer"]
pub struct CreatePartitionsAccepted {
    observer: CreatePartitionsObserver,
    fault: Option<CreatePartitionsAcceptedFaultKind>,
}

impl CreatePartitionsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<CreatePartitionsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> CreatePartitionsObserver {
        self.observer
    }
}

impl fmt::Debug for CreatePartitionsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatePartitionsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
