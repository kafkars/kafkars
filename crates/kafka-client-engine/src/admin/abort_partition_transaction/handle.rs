//! Capture-first runtime-neutral admission of one partition transaction abort.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AbortPartitionTransactionAdmissionError, AbortPartitionTransactionAdmissionErrorKind,
    AbortPartitionTransactionObserver, AbortPartitionTransactionRequest,
};

impl AdminHandle {
    /// Captures one deadline, validates the specification, and attempts admission.
    pub fn try_abort_partition_transaction(
        &self,
        request: AbortPartitionTransactionRequest,
        timeout: Duration,
    ) -> Result<AbortPartitionTransactionAccepted, AbortPartitionTransactionAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(AbortPartitionTransactionAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(admission(
                AbortPartitionTransactionAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.canonicalize().into_plan().map_err(|_error| {
            admission(AbortPartitionTransactionAdmissionErrorKind::InvalidRequest)
        })?;
        let admitted = self
            .abort_partition_transaction
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(admission)?;
        Ok(AbortPartitionTransactionAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

const fn admission(
    kind: AbortPartitionTransactionAdmissionErrorKind,
) -> AbortPartitionTransactionAdmissionError {
    AbortPartitionTransactionAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::AbortPartitionTransactionHostError,
) -> AbortPartitionTransactionAcceptedFaultKind {
    match fault {
        super::AbortPartitionTransactionHostError::Wake => {
            AbortPartitionTransactionAcceptedFaultKind::Wake
        }
        _ => AbortPartitionTransactionAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortPartitionTransactionAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted partition transaction-abort work must retain its observer"]
pub struct AbortPartitionTransactionAccepted {
    observer: AbortPartitionTransactionObserver,
    fault: Option<AbortPartitionTransactionAcceptedFaultKind>,
}

impl AbortPartitionTransactionAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<AbortPartitionTransactionAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> AbortPartitionTransactionObserver {
        self.observer
    }
}

impl fmt::Debug for AbortPartitionTransactionAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AbortPartitionTransactionAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
