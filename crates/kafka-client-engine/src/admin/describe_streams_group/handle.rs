//! Capture-first runtime-neutral admission of one streams-group description.

use std::{fmt, time::Duration};

use kafka_client_core::DescribeStreamsGroupPlan;

use crate::admin::AdminHandle;

use super::{
    DescribeStreamsGroupAdmissionError, DescribeStreamsGroupAdmissionErrorKind,
    DescribeStreamsGroupObserver, DescribeStreamsGroupRequest, DescribeStreamsGroupsRequest,
};

impl AdminHandle {
    /// Captures the call-boundary deadline before validating exact group intent.
    pub fn try_describe_streams_group(
        &self,
        request: DescribeStreamsGroupRequest,
        timeout: Duration,
    ) -> Result<DescribeStreamsGroupAccepted, DescribeStreamsGroupAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_id, include_authorized_operations, include_topology_description) =
            request.canonicalize().into_parts();
        let plan = DescribeStreamsGroupPlan::new(
            group_id,
            include_authorized_operations,
            include_topology_description,
        )
        .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::InvalidRequest))?;
        let now = self
            .clock
            .now()
            .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::HostUnavailable))?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .describe_streams_group
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(DescribeStreamsGroupAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }

    /// Captures one call-boundary deadline before validating the caller-ordered batch.
    pub fn try_describe_streams_groups(
        &self,
        request: DescribeStreamsGroupsRequest,
        timeout: Duration,
    ) -> Result<DescribeStreamsGroupAccepted, DescribeStreamsGroupAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline))?;
        if timeout.is_zero() {
            return Err(admission(
                DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let (group_ids, include_authorized_operations, include_topology_description) =
            request.canonicalize().into_parts();
        let plan = DescribeStreamsGroupPlan::new_batch(
            group_ids,
            include_authorized_operations,
            include_topology_description,
        )
        .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::InvalidRequest))?;
        let now = self
            .clock
            .now()
            .map_err(|_error| admission(DescribeStreamsGroupAdmissionErrorKind::HostUnavailable))?;
        if capture.deadline().is_elapsed_at(now) {
            return Err(admission(
                DescribeStreamsGroupAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admitted = self
            .describe_streams_group
            .try_admit(now, capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(DescribeStreamsGroupAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(
    kind: DescribeStreamsGroupAdmissionErrorKind,
) -> DescribeStreamsGroupAdmissionError {
    DescribeStreamsGroupAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::DescribeStreamsGroupHostError,
) -> DescribeStreamsGroupAcceptedFaultKind {
    match fault {
        super::DescribeStreamsGroupHostError::Wake => DescribeStreamsGroupAcceptedFaultKind::Wake,
        _ => DescribeStreamsGroupAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted DescribeStreamsGroup work must retain its observer"]
pub struct DescribeStreamsGroupAccepted {
    observer: DescribeStreamsGroupObserver,
    fault: Option<DescribeStreamsGroupAcceptedFaultKind>,
}

impl DescribeStreamsGroupAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<DescribeStreamsGroupAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeStreamsGroupObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeStreamsGroupAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
