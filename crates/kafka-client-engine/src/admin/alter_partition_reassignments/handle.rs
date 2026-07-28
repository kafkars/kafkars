//! Runtime-neutral admission at the reassignment public call boundary.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AlterPartitionReassignmentsAdmissionError, AlterPartitionReassignmentsAdmissionErrorKind,
    AlterPartitionReassignmentsObserver, AlterPartitionReassignmentsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission and captures the sole deadline.
    pub fn try_alter_partition_reassignments(
        &self,
        request: AlterPartitionReassignmentsRequest,
        timeout: Duration,
    ) -> Result<AlterPartitionReassignmentsAccepted, AlterPartitionReassignmentsAdmissionError>
    {
        let capture = match self.clock.capture_deadline_after(timeout) {
            Ok(capture) => capture,
            Err(_error) => {
                return Err(AlterPartitionReassignmentsAdmissionError::new(
                    AlterPartitionReassignmentsAdmissionErrorKind::InvalidDeadline,
                    request,
                ));
            }
        };
        if timeout.is_zero() {
            return Err(AlterPartitionReassignmentsAdmissionError::new(
                AlterPartitionReassignmentsAdmissionErrorKind::InvalidDeadline,
                request,
            ));
        }
        if !request.preparation_charge().is_some_and(|charge| {
            charge <= super::host::ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES
        }) {
            return Err(AlterPartitionReassignmentsAdmissionError::new(
                AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes,
                request,
            ));
        }
        let plan = request
            .clone()
            .canonicalize()
            .into_plan()
            .map_err(|_error| {
                AlterPartitionReassignmentsAdmissionError::new(
                    AlterPartitionReassignmentsAdmissionErrorKind::InvalidRequest,
                    request.clone(),
                )
            })?;
        let admission = self
            .alter_partition_reassignments
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(|kind| AlterPartitionReassignmentsAdmissionError::new(kind, request))?;
        Ok(AlterPartitionReassignmentsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::AlterPartitionReassignmentsHostError,
) -> AlterPartitionReassignmentsAcceptedFaultKind {
    match fault {
        super::AlterPartitionReassignmentsHostError::Wake => {
            AlterPartitionReassignmentsAcceptedFaultKind::Wake
        }
        _ => AlterPartitionReassignmentsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterPartitionReassignmentsAcceptedFaultKind {
    /// Reactor notification failed after ownership committed.
    Wake,
    /// The retained host reported an invariant failure after admission.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted AlterPartitionReassignments work must retain its observer"]
pub struct AlterPartitionReassignmentsAccepted {
    observer: AlterPartitionReassignmentsObserver,
    fault: Option<AlterPartitionReassignmentsAcceptedFaultKind>,
}

impl AlterPartitionReassignmentsAccepted {
    /// Returns any post-commit degradation observed during admission.
    pub const fn fault(&self) -> Option<AlterPartitionReassignmentsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the accepted value into its single terminal observer.
    pub fn into_observer(self) -> AlterPartitionReassignmentsObserver {
        self.observer
    }
}

impl fmt::Debug for AlterPartitionReassignmentsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterPartitionReassignmentsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
