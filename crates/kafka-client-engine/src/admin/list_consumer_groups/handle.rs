//! Runtime-neutral bounded admission for cluster-wide consumer-group listing.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    ListConsumerGroupsAdmissionError, ListConsumerGroupsAdmissionErrorKind,
    ListConsumerGroupsHostError, ListConsumerGroupsObserver,
};

impl AdminHandle {
    /// Captures one public deadline and attempts immediate bounded admission.
    pub fn try_list_consumer_groups(
        &self,
        timeout: Duration,
    ) -> Result<ListConsumerGroupsAccepted, ListConsumerGroupsAdmissionError> {
        let capture = self.clock.capture_deadline_after(timeout).map_err(|_| {
            ListConsumerGroupsAdmissionError::new(
                ListConsumerGroupsAdmissionErrorKind::InvalidDeadline,
            )
        })?;
        if timeout.is_zero() {
            return Err(ListConsumerGroupsAdmissionError::new(
                ListConsumerGroupsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .list_consumer_groups
            .try_admit(capture.now(), capture.operation_deadline())
            .map_err(ListConsumerGroupsAdmissionError::new)?;
        Ok(ListConsumerGroupsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: ListConsumerGroupsHostError,
) -> ListConsumerGroupsAcceptedFaultKind {
    match fault {
        ListConsumerGroupsHostError::Wake => ListConsumerGroupsAcceptedFaultKind::Wake,
        _ => ListConsumerGroupsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke operation ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListConsumerGroupsAcceptedFaultKind {
    /// The operation was accepted but waking its host failed.
    Wake,
    /// The operation was accepted but its host reported an invariant failure.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted ListConsumerGroups work must retain its observer"]
pub struct ListConsumerGroupsAccepted {
    observer: ListConsumerGroupsObserver,
    fault: Option<ListConsumerGroupsAcceptedFaultKind>,
}

impl ListConsumerGroupsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<ListConsumerGroupsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> ListConsumerGroupsObserver {
        self.observer
    }
}

impl fmt::Debug for ListConsumerGroupsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
