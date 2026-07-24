//! Runtime-neutral admission of concrete `DescribeCluster` work.

use std::{fmt, time::Duration};

use super::{
    AdminHandle, DescribeClusterAdmissionError, DescribeClusterAdmissionErrorKind,
    DescribeClusterHostError, DescribeClusterObserver,
};

impl AdminHandle {
    /// Attempts immediate bounded admission at one public call boundary.
    pub fn try_describe_cluster(
        &self,
        timeout: Duration,
    ) -> Result<DescribeClusterAccepted, DescribeClusterAdmissionError> {
        self.try_describe_cluster_with_options(false, false, timeout)
    }

    /// Attempts one cluster description with explicit fenced-broker visibility.
    pub fn try_describe_cluster_with_fenced_brokers(
        &self,
        include_fenced_brokers: bool,
        timeout: Duration,
    ) -> Result<DescribeClusterAccepted, DescribeClusterAdmissionError> {
        self.try_describe_cluster_with_options(include_fenced_brokers, false, timeout)
    }

    /// Attempts one cluster description with both explicit cluster-view policies.
    pub fn try_describe_cluster_with_options(
        &self,
        include_fenced_brokers: bool,
        include_authorized_operations: bool,
        timeout: Duration,
    ) -> Result<DescribeClusterAccepted, DescribeClusterAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeClusterAdmissionError::new(
                    DescribeClusterAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeClusterAdmissionError::new(
                DescribeClusterAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let admission = self
            .describe_cluster
            .try_admit_with_options(
                capture.now(),
                capture.operation_deadline(),
                include_fenced_brokers,
                include_authorized_operations,
            )
            .map_err(DescribeClusterAdmissionError::new)?;
        Ok(DescribeClusterAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: DescribeClusterHostError,
) -> DescribeClusterAcceptedFaultKind {
    match fault {
        DescribeClusterHostError::Wake => DescribeClusterAcceptedFaultKind::Wake,
        _ => DescribeClusterAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeClusterAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeCluster work must retain its observer"]
pub struct DescribeClusterAccepted {
    observer: DescribeClusterObserver,
    fault: Option<DescribeClusterAcceptedFaultKind>,
}

impl DescribeClusterAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeClusterAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeClusterObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeClusterAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeClusterAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
