//! Runtime-neutral admission of one Admin feature description.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeFeaturesAdmissionError, DescribeFeaturesAdmissionErrorKind, DescribeFeaturesObserver,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_features(
        &self,
        timeout: Duration,
    ) -> Result<DescribeFeaturesAccepted, DescribeFeaturesAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeFeaturesAdmissionError::new(
                    DescribeFeaturesAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeFeaturesAdmissionError::new(
                DescribeFeaturesAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .describe_features
            .try_admit(capture.now(), capture.operation_deadline())
            .map_err(DescribeFeaturesAdmissionError::new)?;
        Ok(DescribeFeaturesAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::DescribeFeaturesHostError,
) -> DescribeFeaturesAcceptedFaultKind {
    match fault {
        super::DescribeFeaturesHostError::Wake => DescribeFeaturesAcceptedFaultKind::Wake,
        _ => DescribeFeaturesAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeFeaturesAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted DescribeFeatures work must retain its observer"]
pub struct DescribeFeaturesAccepted {
    observer: DescribeFeaturesObserver,
    fault: Option<DescribeFeaturesAcceptedFaultKind>,
}

impl DescribeFeaturesAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<DescribeFeaturesAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeFeaturesObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeFeaturesAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeFeaturesAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
