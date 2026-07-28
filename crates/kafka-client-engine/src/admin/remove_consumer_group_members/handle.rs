//! Runtime-neutral admission at the member-removal public call boundary.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    RemoveConsumerGroupMembersAdmissionError, RemoveConsumerGroupMembersAdmissionErrorKind,
    RemoveConsumerGroupMembersObserver, RemoveConsumerGroupMembersRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded admission and captures the sole deadline.
    pub fn try_remove_consumer_group_members(
        &self,
        request: RemoveConsumerGroupMembersRequest,
        timeout: Duration,
    ) -> Result<RemoveConsumerGroupMembersAccepted, RemoveConsumerGroupMembersAdmissionError> {
        let capture = match self.clock.capture_deadline_after(timeout) {
            Ok(capture) => capture,
            Err(_error) => {
                return Err(RemoveConsumerGroupMembersAdmissionError::new(
                    RemoveConsumerGroupMembersAdmissionErrorKind::InvalidDeadline,
                    request,
                ));
            }
        };
        if timeout.is_zero() {
            return Err(RemoveConsumerGroupMembersAdmissionError::new(
                RemoveConsumerGroupMembersAdmissionErrorKind::InvalidDeadline,
                request,
            ));
        }
        if !request.preparation_charge().is_some_and(|charge| {
            charge <= super::host::REMOVE_CONSUMER_GROUP_MEMBERS_RETAINED_BYTES
        }) {
            return Err(RemoveConsumerGroupMembersAdmissionError::new(
                RemoveConsumerGroupMembersAdmissionErrorKind::RetainedBytes,
                request,
            ));
        }
        let plan = request
            .clone()
            .canonicalize()
            .into_plan()
            .map_err(|_error| {
                RemoveConsumerGroupMembersAdmissionError::new(
                    RemoveConsumerGroupMembersAdmissionErrorKind::InvalidRequest,
                    request.clone(),
                )
            })?;
        let admission = self
            .remove_consumer_group_members
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(|kind| RemoveConsumerGroupMembersAdmissionError::new(kind, request))?;
        Ok(RemoveConsumerGroupMembersAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: super::RemoveConsumerGroupMembersHostError,
) -> RemoveConsumerGroupMembersAcceptedFaultKind {
    match fault {
        super::RemoveConsumerGroupMembersHostError::Wake => {
            RemoveConsumerGroupMembersAcceptedFaultKind::Wake
        }
        _ => RemoveConsumerGroupMembersAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveConsumerGroupMembersAcceptedFaultKind {
    /// Reactor notification failed after ownership committed.
    Wake,
    /// The retained host reported an invariant failure after admission.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted RemoveConsumerGroupMembers work must retain its observer"]
pub struct RemoveConsumerGroupMembersAccepted {
    observer: RemoveConsumerGroupMembersObserver,
    fault: Option<RemoveConsumerGroupMembersAcceptedFaultKind>,
}

impl RemoveConsumerGroupMembersAccepted {
    /// Returns any post-commit degradation observed during admission.
    pub const fn fault(&self) -> Option<RemoveConsumerGroupMembersAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the accepted value into its single terminal observer.
    pub fn into_observer(self) -> RemoveConsumerGroupMembersObserver {
        self.observer
    }
}

impl fmt::Debug for RemoveConsumerGroupMembersAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveConsumerGroupMembersAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
