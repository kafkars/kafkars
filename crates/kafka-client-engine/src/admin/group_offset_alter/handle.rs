//! Runtime-neutral admission of one concrete offset-alteration request.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    AlterConsumerGroupOffsetsAdmissionError, AlterConsumerGroupOffsetsAdmissionErrorKind,
    AlterConsumerGroupOffsetsObserver, AlterConsumerGroupOffsetsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_alter_consumer_group_offsets(
        &self,
        request: AlterConsumerGroupOffsetsRequest,
        timeout: Duration,
    ) -> Result<AlterConsumerGroupOffsetsAccepted, AlterConsumerGroupOffsetsAdmissionError> {
        let capture = match self.clock.capture_deadline_after(timeout) {
            Ok(capture) => capture,
            Err(_error) => {
                return Err(AlterConsumerGroupOffsetsAdmissionError::new(
                    AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
                    request,
                ));
            }
        };
        if timeout.is_zero() {
            return Err(AlterConsumerGroupOffsetsAdmissionError::new(
                AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
                request,
            ));
        }
        let preparation_fits = request.preparation_charge().is_some_and(|charge| {
            charge <= super::host::ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
        });
        if !preparation_fits {
            return Err(AlterConsumerGroupOffsetsAdmissionError::new(
                AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes,
                request,
            ));
        }
        let plan = match request.clone().canonicalize().into_plan() {
            Ok(plan) => plan,
            Err(_error) => {
                return Err(AlterConsumerGroupOffsetsAdmissionError::new(
                    AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
                    request,
                ));
            }
        };
        let admission = match self.alter_consumer_group_offsets.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan,
        ) {
            Ok(admission) => admission,
            Err(kind) => {
                return Err(AlterConsumerGroupOffsetsAdmissionError::new(kind, request));
            }
        };
        Ok(AlterConsumerGroupOffsetsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::AlterConsumerGroupOffsetsHostError,
) -> AlterConsumerGroupOffsetsAcceptedFaultKind {
    match fault {
        super::AlterConsumerGroupOffsetsHostError::Wake => {
            AlterConsumerGroupOffsetsAcceptedFaultKind::Wake
        }
        super::AlterConsumerGroupOffsetsHostError::Machine(_)
        | super::AlterConsumerGroupOffsetsHostError::Completion(_)
        | super::AlterConsumerGroupOffsetsHostError::UnknownOperation
        | super::AlterConsumerGroupOffsetsHostError::MissingSubmission
        | super::AlterConsumerGroupOffsetsHostError::MissingTerminal
        | super::AlterConsumerGroupOffsetsHostError::SubmissionMismatch
        | super::AlterConsumerGroupOffsetsHostError::InvalidHandoff
        | super::AlterConsumerGroupOffsetsHostError::CallCompletion
        | super::AlterConsumerGroupOffsetsHostError::ByteAccounting
        | super::AlterConsumerGroupOffsetsHostError::Unsettled(_) => {
            AlterConsumerGroupOffsetsAcceptedFaultKind::HostInvariant
        }
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterConsumerGroupOffsetsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted AlterConsumerGroupOffsets work must retain its observer"]
pub struct AlterConsumerGroupOffsetsAccepted {
    observer: AlterConsumerGroupOffsetsObserver,
    fault: Option<AlterConsumerGroupOffsetsAcceptedFaultKind>,
}

impl AlterConsumerGroupOffsetsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AlterConsumerGroupOffsetsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AlterConsumerGroupOffsetsObserver {
        self.observer
    }
}

impl fmt::Debug for AlterConsumerGroupOffsetsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterConsumerGroupOffsetsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
