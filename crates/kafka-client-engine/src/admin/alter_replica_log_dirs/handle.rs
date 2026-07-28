//! Runtime-neutral admission of one concrete Admin `AlterReplicaLogDirs` mutation.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AlterReplicaLogDirsAdmissionError, AlterReplicaLogDirsAdmissionErrorKind,
    AlterReplicaLogDirsObserver, AlterReplicaLogDirsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_alter_replica_log_dirs(
        &self,
        request: AlterReplicaLogDirsRequest,
        timeout: Duration,
    ) -> Result<AlterReplicaLogDirsAccepted, AlterReplicaLogDirsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AlterReplicaLogDirsAdmissionError::new(
                    AlterReplicaLogDirsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(AlterReplicaLogDirsAdmissionError::new(
                AlterReplicaLogDirsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            AlterReplicaLogDirsAdmissionError::new(
                AlterReplicaLogDirsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .alter_replica_log_dirs
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(AlterReplicaLogDirsAdmissionError::new)?;
        Ok(AlterReplicaLogDirsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::AlterReplicaLogDirsHostError,
) -> AlterReplicaLogDirsAcceptedFaultKind {
    match fault {
        super::AlterReplicaLogDirsHostError::Wake => AlterReplicaLogDirsAcceptedFaultKind::Wake,
        super::AlterReplicaLogDirsHostError::Machine(_)
        | super::AlterReplicaLogDirsHostError::Completion(_)
        | super::AlterReplicaLogDirsHostError::UnknownOperation
        | super::AlterReplicaLogDirsHostError::MissingSubmission
        | super::AlterReplicaLogDirsHostError::MissingTerminal
        | super::AlterReplicaLogDirsHostError::SubmissionMismatch
        | super::AlterReplicaLogDirsHostError::InvalidHandoff
        | super::AlterReplicaLogDirsHostError::CallCompletion
        | super::AlterReplicaLogDirsHostError::ByteAccounting
        | super::AlterReplicaLogDirsHostError::Unsettled(_) => {
            AlterReplicaLogDirsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterReplicaLogDirsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted Admin AlterReplicaLogDirs work must retain its observer"]
pub struct AlterReplicaLogDirsAccepted {
    observer: AlterReplicaLogDirsObserver,
    fault: Option<AlterReplicaLogDirsAcceptedFaultKind>,
}

impl AlterReplicaLogDirsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AlterReplicaLogDirsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AlterReplicaLogDirsObserver {
        self.observer
    }
}

impl fmt::Debug for AlterReplicaLogDirsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterReplicaLogDirsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
