//! Runtime-neutral public handle retaining concrete admin admission ports.

use std::{fmt, sync::Arc, time::Duration};

use super::{
    CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind, CreateTopicsObserver,
    CreateTopicsRequest, DeleteTopicsAdmissionPort, shard::CreateTopicsAdmissionPort,
};
use crate::clock::MonotonicClock;

/// Cheaply cloneable handle to the concrete admin shards.
#[derive(Clone)]
pub struct AdminHandle {
    pub(super) create_topics: CreateTopicsAdmissionPort,
    pub(super) delete_topics: DeleteTopicsAdmissionPort,
    pub(super) describe_cluster: super::DescribeClusterAdmissionPort,
    pub(super) clock: Arc<MonotonicClock>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl AdminHandle {
    pub(crate) fn new(
        create_topics: CreateTopicsAdmissionPort,
        delete_topics: DeleteTopicsAdmissionPort,
        describe_cluster: super::DescribeClusterAdmissionPort,
        clock: Arc<MonotonicClock>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            create_topics,
            delete_topics,
            describe_cluster,
            clock,
            _lifetime: lifetime,
        }
    }

    /// Attempts immediate bounded admission using one call-boundary deadline.
    pub fn try_create_topics(
        &self,
        request: CreateTopicsRequest,
        timeout: Duration,
    ) -> Result<CreateTopicsAccepted, CreateTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(CreateTopicsAdmissionError::new(
                CreateTopicsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .create_topics
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(CreateTopicsAdmissionError::new)?;
        Ok(CreateTopicsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for AdminHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminHandle")
            .finish_non_exhaustive()
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::CreateTopicsHostError,
) -> CreateTopicsAcceptedFaultKind {
    match fault {
        super::CreateTopicsHostError::Wake => CreateTopicsAcceptedFaultKind::Wake,
        _ => CreateTopicsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke operation ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted CreateTopics work must retain its observer"]
pub struct CreateTopicsAccepted {
    observer: CreateTopicsObserver,
    fault: Option<CreateTopicsAcceptedFaultKind>,
}

impl CreateTopicsAccepted {
    /// Returns any post-commit degradation without misclassifying ownership.
    pub const fn fault(&self) -> Option<CreateTopicsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> CreateTopicsObserver {
        self.observer
    }
}

impl fmt::Debug for CreateTopicsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateTopicsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
