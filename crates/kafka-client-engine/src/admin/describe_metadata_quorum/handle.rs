//! Runtime-neutral admission of one concrete Admin `DescribeMetadataQuorum` query.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeMetadataQuorumAdmissionError, DescribeMetadataQuorumAdmissionErrorKind,
    DescribeMetadataQuorumObserver,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_metadata_quorum(
        &self,
        timeout: Duration,
    ) -> Result<DescribeMetadataQuorumAccepted, DescribeMetadataQuorumAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeMetadataQuorumAdmissionError::new(
                    DescribeMetadataQuorumAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeMetadataQuorumAdmissionError::new(
                DescribeMetadataQuorumAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .describe_metadata_quorum
            .try_admit(capture.now(), capture.operation_deadline())
            .map_err(DescribeMetadataQuorumAdmissionError::new)?;
        Ok(DescribeMetadataQuorumAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeMetadataQuorumHostError,
) -> DescribeMetadataQuorumAcceptedFaultKind {
    match fault {
        super::DescribeMetadataQuorumHostError::Wake => {
            DescribeMetadataQuorumAcceptedFaultKind::Wake
        }
        _ => DescribeMetadataQuorumAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeMetadataQuorumAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeMetadataQuorum work must retain its observer"]
pub struct DescribeMetadataQuorumAccepted {
    observer: DescribeMetadataQuorumObserver,
    fault: Option<DescribeMetadataQuorumAcceptedFaultKind>,
}

impl DescribeMetadataQuorumAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeMetadataQuorumAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeMetadataQuorumObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeMetadataQuorumAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeMetadataQuorumAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
