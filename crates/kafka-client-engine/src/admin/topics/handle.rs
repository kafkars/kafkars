//! Runtime-neutral admission of concrete `DescribeTopics` work.

use std::{
    fmt,
    time::{Duration, Instant},
};

use crate::{admin::AdminHandle, clock::DeadlineCapture};

use super::{
    DescribeTopicsAdmissionError, DescribeTopicsAdmissionErrorKind, DescribeTopicsObserver,
    DescribeTopicsRequest,
};

impl AdminHandle {
    /// Attempts immediate bounded topic-description admission at one call boundary.
    pub fn try_describe_topics(
        &self,
        request: DescribeTopicsRequest,
        timeout: Duration,
    ) -> Result<DescribeTopicsAccepted, DescribeTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| invalid_deadline())?;
        if timeout.is_zero() {
            return Err(invalid_deadline());
        }
        self.try_describe_topics_with_capture(request, capture)
    }

    /// Attempts one topic description under an already-captured deadline.
    ///
    /// This is a narrow cross-crate seam for a facade that owns the public
    /// timing boundary. The ordinary duration API remains the supported
    /// application-facing entry point.
    #[doc(hidden)]
    pub fn try_describe_topics_until(
        &self,
        request: DescribeTopicsRequest,
        deadline: Instant,
    ) -> Result<DescribeTopicsAccepted, DescribeTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_until(deadline)
            .map_err(|_error| invalid_deadline())?;
        self.try_describe_topics_with_capture(request, capture)
    }

    fn try_describe_topics_with_capture(
        &self,
        request: DescribeTopicsRequest,
        capture: DeadlineCapture,
    ) -> Result<DescribeTopicsAccepted, DescribeTopicsAdmissionError> {
        if capture.deadline().is_elapsed_at(capture.now()) {
            return Err(invalid_deadline());
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            DescribeTopicsAdmissionError::new(DescribeTopicsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            DescribeTopicsAdmissionError::new(DescribeTopicsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .describe_topics
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(DescribeTopicsAdmissionError::new)?;
        Ok(DescribeTopicsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

fn invalid_deadline() -> DescribeTopicsAdmissionError {
    DescribeTopicsAdmissionError::new(DescribeTopicsAdmissionErrorKind::InvalidDeadline)
}

const fn accepted_fault_kind(
    fault: super::DescribeTopicsHostError,
) -> DescribeTopicsAcceptedFaultKind {
    match fault {
        super::DescribeTopicsHostError::Wake => DescribeTopicsAcceptedFaultKind::Wake,
        _ => DescribeTopicsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeTopicsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeTopics work must retain its observer"]
pub struct DescribeTopicsAccepted {
    observer: DescribeTopicsObserver,
    fault: Option<DescribeTopicsAcceptedFaultKind>,
}

impl DescribeTopicsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeTopicsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeTopicsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeTopicsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeTopicsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
