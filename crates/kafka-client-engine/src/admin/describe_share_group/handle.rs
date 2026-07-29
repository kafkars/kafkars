//! Capture-first runtime-neutral admission of one share-group description.

use std::{fmt, time::Duration};

use kafka_client_core::DescribeShareGroupPlan;

use crate::admin::AdminHandle;

use super::{
    DescribeShareGroupAdmissionError, DescribeShareGroupAdmissionErrorKind,
    DescribeShareGroupObserver, DescribeShareGroupRequest, DescribeShareGroupsRequest,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating exact group intent.
    pub fn try_describe_share_group(
        &self,
        request: DescribeShareGroupRequest,
        timeout: Duration,
    ) -> Result<DescribeShareGroupAccepted, DescribeShareGroupAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                DescribeShareGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_id, include_authorized_operations) = request.canonicalize().into_parts();
        let plan = DescribeShareGroupPlan::new(group_id, include_authorized_operations)
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::InvalidRequest))?;
        let now = self
            .clock
            .now()
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::HostUnavailable))?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                DescribeShareGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .describe_share_group
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(DescribeShareGroupAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }

    /// Captures one deadline before validating a caller-ordered group batch.
    pub fn try_describe_share_groups(
        &self,
        request: DescribeShareGroupsRequest,
        timeout: Duration,
    ) -> Result<DescribeShareGroupAccepted, DescribeShareGroupAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                DescribeShareGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_ids, include_authorized_operations) = request.canonicalize().into_parts();
        let plan = DescribeShareGroupPlan::new_batch(group_ids, include_authorized_operations)
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::InvalidRequest))?;
        let now = self
            .clock
            .now()
            .map_err(|_error| admission(DescribeShareGroupAdmissionErrorKind::HostUnavailable))?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                DescribeShareGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .describe_share_group
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(DescribeShareGroupAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(kind: DescribeShareGroupAdmissionErrorKind) -> DescribeShareGroupAdmissionError {
    DescribeShareGroupAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::DescribeShareGroupHostError,
) -> DescribeShareGroupAcceptedFaultKind {
    match fault {
        super::DescribeShareGroupHostError::Wake => DescribeShareGroupAcceptedFaultKind::Wake,
        _ => DescribeShareGroupAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted DescribeShareGroup work must retain its observer"]
pub struct DescribeShareGroupAccepted {
    observer: DescribeShareGroupObserver,
    fault: Option<DescribeShareGroupAcceptedFaultKind>,
}

impl DescribeShareGroupAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<DescribeShareGroupAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeShareGroupObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeShareGroupAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
