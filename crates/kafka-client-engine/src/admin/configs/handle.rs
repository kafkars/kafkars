//! Runtime-neutral admission for concrete topic `DescribeConfigs` work.

use std::{fmt, time::Duration};

use crate::admin::AdminHandle;

use super::{
    DescribeConfigsAdmissionError, DescribeConfigsAdmissionErrorKind, DescribeConfigsObserver,
    DescribeConfigsRequest, model::DescribeConfigsRequestError,
};

impl AdminHandle {
    /// Attempts immediate bounded topic-configuration admission.
    pub fn try_describe_configs(
        &self,
        request: DescribeConfigsRequest,
        timeout: Duration,
    ) -> Result<DescribeConfigsAccepted, DescribeConfigsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                DescribeConfigsAdmissionError::new(
                    DescribeConfigsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        if timeout.is_zero() {
            return Err(DescribeConfigsAdmissionError::new(
                DescribeConfigsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retention = request.retention().ok_or_else(|| {
            DescribeConfigsAdmissionError::new(DescribeConfigsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_topic_plan().map_err(|error| {
            DescribeConfigsAdmissionError::new(match error {
                DescribeConfigsRequestError::Invalid(_error) => {
                    DescribeConfigsAdmissionErrorKind::InvalidRequest
                }
                DescribeConfigsRequestError::UnsupportedResource => {
                    DescribeConfigsAdmissionErrorKind::UnsupportedResource
                }
            })
        })?;
        let admission = self
            .describe_configs
            .try_admit(capture.now(), capture.operation_deadline(), plan, retention)
            .map_err(DescribeConfigsAdmissionError::new)?;
        Ok(DescribeConfigsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::DescribeConfigsHostError,
) -> DescribeConfigsAcceptedFaultKind {
    match fault {
        super::DescribeConfigsHostError::Wake => DescribeConfigsAcceptedFaultKind::Wake,
        _ => DescribeConfigsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConfigsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted DescribeConfigs work must retain its observer"]
pub struct DescribeConfigsAccepted {
    observer: DescribeConfigsObserver,
    fault: Option<DescribeConfigsAcceptedFaultKind>,
}

impl DescribeConfigsAccepted {
    /// Returns any post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<DescribeConfigsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> DescribeConfigsObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeConfigsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeConfigsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
