//! Runtime-neutral admission of one Admin `DescribeTopicPartitions` page.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AdminDescribeTopicPartitionsAdmissionError, AdminDescribeTopicPartitionsAdmissionErrorKind,
    AdminDescribeTopicPartitionsObserver, AdminDescribeTopicPartitionsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_topic_partitions(
        &self,
        request: AdminDescribeTopicPartitionsRequest,
        timeout: Duration,
    ) -> Result<AdminDescribeTopicPartitionsAccepted, AdminDescribeTopicPartitionsAdmissionError>
    {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AdminDescribeTopicPartitionsAdmissionError::new(
                    AdminDescribeTopicPartitionsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AdminDescribeTopicPartitionsAdmissionError::new(
                AdminDescribeTopicPartitionsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AdminDescribeTopicPartitionsAdmissionError::new(
                AdminDescribeTopicPartitionsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_topic_partitions
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AdminDescribeTopicPartitionsAdmissionError::new)?;
        Ok(AdminDescribeTopicPartitionsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AdminDescribeTopicPartitionsHostError,
) -> AdminDescribeTopicPartitionsAcceptedFaultKind {
    match fault {
        super::AdminDescribeTopicPartitionsHostError::Wake => {
            AdminDescribeTopicPartitionsAcceptedFaultKind::Wake
        }
        _ => AdminDescribeTopicPartitionsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeTopicPartitionsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted one-page operation plus advisory post-commit degradation.
#[must_use = "accepted DescribeTopicPartitions work must retain its observer"]
pub struct AdminDescribeTopicPartitionsAccepted {
    observer: AdminDescribeTopicPartitionsObserver,
    fault: Option<AdminDescribeTopicPartitionsAcceptedFaultKind>,
}

impl AdminDescribeTopicPartitionsAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<AdminDescribeTopicPartitionsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> AdminDescribeTopicPartitionsObserver {
        self.observer
    }
}

impl fmt::Debug for AdminDescribeTopicPartitionsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminDescribeTopicPartitionsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
