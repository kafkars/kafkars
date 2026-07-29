//! Runtime-neutral bounded admission for consumer-group description.

use std::{fmt, time::Duration};

use kafka_client_core::AdminDescribeConsumerGroupsScope;

use crate::admin::AdminHandle;

use super::{
    DescribeConsumerGroupsAdmissionError, DescribeConsumerGroupsAdmissionErrorKind,
    DescribeConsumerGroupsHostError, DescribeConsumerGroupsObserver, DescribeConsumerGroupsRequest,
};

impl AdminHandle {
    /// Captures one public deadline and attempts immediate bounded admission.
    pub fn try_describe_consumer_groups(
        &self,
        request: DescribeConsumerGroupsRequest,
        timeout: Duration,
    ) -> Result<DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError> {
        self.try_describe_consumer_groups_with_scope(
            request,
            timeout,
            AdminDescribeConsumerGroupsScope::ModernFirst,
        )
    }

    /// Uses classic `DescribeGroups` directly on the existing bounded owner.
    pub fn try_describe_classic_groups(
        &self,
        request: DescribeConsumerGroupsRequest,
        timeout: Duration,
    ) -> Result<DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError> {
        self.try_describe_consumer_groups_with_scope(
            request,
            timeout,
            AdminDescribeConsumerGroupsScope::ClassicOnly,
        )
    }

    fn try_describe_consumer_groups_with_scope(
        &self,
        request: DescribeConsumerGroupsRequest,
        timeout: Duration,
        scope: AdminDescribeConsumerGroupsScope,
    ) -> Result<DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError> {
        let capture = self.clock.capture_deadline_after(timeout).map_err(|_| {
            DescribeConsumerGroupsAdmissionError::new(
                DescribeConsumerGroupsAdmissionErrorKind::InvalidDeadline,
            )
        })?;
        if timeout.is_zero() {
            return Err(DescribeConsumerGroupsAdmissionError::new(
                DescribeConsumerGroupsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let plan = match scope {
            AdminDescribeConsumerGroupsScope::ModernFirst => request.into_plan(),
            AdminDescribeConsumerGroupsScope::ClassicOnly => request.into_plan_with_scope(scope),
        }
        .map_err(|_| {
            DescribeConsumerGroupsAdmissionError::new(
                DescribeConsumerGroupsAdmissionErrorKind::InvalidRequest,
            )
        })?;
        let admission = self
            .describe_consumer_groups
            .try_admit(capture.now(), capture.operation_deadline(), plan)
            .map_err(DescribeConsumerGroupsAdmissionError::new)?;
        Ok(DescribeConsumerGroupsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

const fn accepted_fault_kind(
    fault: DescribeConsumerGroupsHostError,
) -> DescribeConsumerGroupsAcceptedFaultKind {
    match fault {
        DescribeConsumerGroupsHostError::Wake => DescribeConsumerGroupsAcceptedFaultKind::Wake,
        _ => DescribeConsumerGroupsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke operation ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsAcceptedFaultKind {
    /// The operation was accepted but waking its host failed.
    Wake,
    /// The operation was accepted but its host reported an invariant failure.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeConsumerGroups work must retain its observer"]
pub struct DescribeConsumerGroupsAccepted {
    observer: DescribeConsumerGroupsObserver,
    fault: Option<DescribeConsumerGroupsAcceptedFaultKind>,
}

impl DescribeConsumerGroupsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeConsumerGroupsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeConsumerGroupsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeConsumerGroupsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeConsumerGroupsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
