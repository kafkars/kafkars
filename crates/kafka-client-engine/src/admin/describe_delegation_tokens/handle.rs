//! Capture-first runtime-neutral admission of one delegation-token description.

use std::{fmt, time::Duration};

use crate::{
    admin::AdminHandle,
    clock::DeadlineCapture,
    protocol::admin::describe_delegation_tokens::{
        DescribeDelegationTokenPrincipalRef, DescribeDelegationTokensRequestFailure,
        DescribeDelegationTokensRequestRef, PreparedDescribeDelegationTokensRequest,
        describe_delegation_tokens_request,
    },
};

use super::{
    DescribeDelegationTokensAdmissionError, DescribeDelegationTokensAdmissionErrorKind,
    DescribeDelegationTokensObserver, DescribeDelegationTokensRequest,
    host::DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES, model::DescribeDelegationTokensPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_describe_delegation_tokens(
        &self,
        timeout: Duration,
    ) -> Result<DescribeDelegationTokensCapture<'_>, DescribeDelegationTokensAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(DescribeDelegationTokensAdmissionErrorKind::InvalidDeadline)
            })?;
        Ok(DescribeDelegationTokensCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned token query.
    pub fn try_describe_delegation_tokens(
        &self,
        request: DescribeDelegationTokensRequest,
        timeout: Duration,
    ) -> Result<DescribeDelegationTokensAccepted, DescribeDelegationTokensAdmissionError> {
        self.capture_describe_delegation_tokens(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting token-description work"]
pub struct DescribeDelegationTokensCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl DescribeDelegationTokensCapture<'_> {
    /// Validates, prepares API key 41, and atomically reserves accepted ownership.
    pub fn try_submit(
        self,
        request: DescribeDelegationTokensRequest,
    ) -> Result<DescribeDelegationTokensAccepted, DescribeDelegationTokensAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                DescribeDelegationTokensAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.plan().map_err(|error| {
            admission(match error {
                DescribeDelegationTokensPlanFailure::Invalid => {
                    DescribeDelegationTokensAdmissionErrorKind::InvalidRequest
                }
                DescribeDelegationTokensPlanFailure::RetainedBytes => {
                    DescribeDelegationTokensAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let prepared = prepare_request(&request)?;
        drop(request);
        let now = self.handle.clock.now().map_err(|_error| {
            admission(DescribeDelegationTokensAdmissionErrorKind::HostInvariant)
        })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                DescribeDelegationTokensAdmissionErrorKind::DeadlineElapsed,
            ));
        }
        let admitted = self
            .handle
            .describe_delegation_tokens
            .try_admit(now, self.deadline.operation_deadline(), plan, prepared)
            .map_err(admission)?;
        Ok(DescribeDelegationTokensAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for DescribeDelegationTokensCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeDelegationTokensCapture")
            .finish_non_exhaustive()
    }
}

fn prepare_request(
    request: &DescribeDelegationTokensRequest,
) -> Result<PreparedDescribeDelegationTokensRequest, DescribeDelegationTokensAdmissionError> {
    let owners = request
        .owners()
        .map(|owners| {
            let mut refs = Vec::new();
            refs.try_reserve_exact(owners.len()).map_err(|_| {
                admission(DescribeDelegationTokensAdmissionErrorKind::RetainedBytes)
            })?;
            refs.extend(owners.iter().map(|owner| {
                DescribeDelegationTokenPrincipalRef::new(
                    owner.principal_type(),
                    owner.principal_name(),
                )
            }));
            Ok(refs)
        })
        .transpose()?;
    let request = match owners.as_deref() {
        Some(owners) => DescribeDelegationTokensRequestRef::selected(owners),
        None => DescribeDelegationTokensRequestRef::all(),
    };
    describe_delegation_tokens_request(request, DESCRIBE_DELEGATION_TOKENS_OPERATION_BYTES)
        .map_err(|error| admission(preparation_error(error)))
}

const fn preparation_error(
    error: DescribeDelegationTokensRequestFailure,
) -> DescribeDelegationTokensAdmissionErrorKind {
    match error {
        DescribeDelegationTokensRequestFailure::RetainedBytes { .. }
        | DescribeDelegationTokensRequestFailure::Allocation { .. } => {
            DescribeDelegationTokensAdmissionErrorKind::RetainedBytes
        }
        _ => DescribeDelegationTokensAdmissionErrorKind::InvalidRequest,
    }
}

const fn admission(
    kind: DescribeDelegationTokensAdmissionErrorKind,
) -> DescribeDelegationTokensAdmissionError {
    DescribeDelegationTokensAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::DescribeDelegationTokensHostError,
) -> DescribeDelegationTokensAcceptedFaultKind {
    match fault {
        super::DescribeDelegationTokensHostError::Wake => {
            DescribeDelegationTokensAcceptedFaultKind::Wake
        }
        _ => DescribeDelegationTokensAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeDelegationTokensAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted DescribeDelegationTokens work must retain its observer"]
pub struct DescribeDelegationTokensAccepted {
    observer: DescribeDelegationTokensObserver,
    fault: Option<DescribeDelegationTokensAcceptedFaultKind>,
}

impl DescribeDelegationTokensAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<DescribeDelegationTokensAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> DescribeDelegationTokensObserver {
        self.observer
    }
}

impl fmt::Debug for DescribeDelegationTokensAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeDelegationTokensAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
