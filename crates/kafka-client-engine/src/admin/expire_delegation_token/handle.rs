//! Capture-first runtime-neutral admission of one delegation-token expiration.

use std::{fmt, time::Duration};

use kafka_client_core::ExpireDelegationTokenPlan;

use crate::{
    admin::AdminHandle,
    clock::DeadlineCapture,
    protocol::admin::expire_delegation_token::{
        ExpireDelegationTokenRequestFailure, ExpireDelegationTokenRequestRef,
        expire_delegation_token_request,
    },
};

use super::{
    ExpireDelegationTokenAdmissionError, ExpireDelegationTokenAdmissionErrorKind,
    ExpireDelegationTokenObserver, ExpireDelegationTokenRequest,
    host::EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES, model::ExpireDelegationTokenPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before secret request conversion.
    pub fn capture_expire_delegation_token(
        &self,
        timeout: Duration,
    ) -> Result<ExpireDelegationTokenCapture<'_>, ExpireDelegationTokenAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(ExpireDelegationTokenAdmissionErrorKind::InvalidDeadline)
            })?;
        Ok(ExpireDelegationTokenCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned token expiration.
    pub fn try_expire_delegation_token(
        &self,
        request: ExpireDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<ExpireDelegationTokenAccepted, ExpireDelegationTokenAdmissionError> {
        self.capture_expire_delegation_token(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting token-expiration work"]
pub struct ExpireDelegationTokenCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl ExpireDelegationTokenCapture<'_> {
    /// Validates, prepares API key 40, and atomically reserves accepted ownership.
    pub fn try_submit(
        self,
        request: ExpireDelegationTokenRequest,
    ) -> Result<ExpireDelegationTokenAccepted, ExpireDelegationTokenAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                ExpireDelegationTokenAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.into_plan().map_err(|error| {
            admission(match error {
                ExpireDelegationTokenPlanFailure::Invalid => {
                    ExpireDelegationTokenAdmissionErrorKind::InvalidRequest
                }
                ExpireDelegationTokenPlanFailure::RetainedBytes => {
                    ExpireDelegationTokenAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let prepared = prepare_request(&plan)?;
        let now =
            self.handle.clock.now().map_err(|_error| {
                admission(ExpireDelegationTokenAdmissionErrorKind::HostInvariant)
            })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                ExpireDelegationTokenAdmissionErrorKind::DeadlineElapsed,
            ));
        }
        let admitted = self
            .handle
            .expire_delegation_token
            .try_admit(now, self.deadline.operation_deadline(), plan, prepared)
            .map_err(admission)?;
        Ok(ExpireDelegationTokenAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for ExpireDelegationTokenCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenCapture")
            .finish_non_exhaustive()
    }
}

fn prepare_request(
    plan: &ExpireDelegationTokenPlan,
) -> Result<
    crate::protocol::admin::expire_delegation_token::PreparedExpireDelegationTokenRequest,
    ExpireDelegationTokenAdmissionError,
> {
    let hmac = plan.hmac().as_bytes();
    let request = match plan.expiry_period_ms() {
        Some(period) => ExpireDelegationTokenRequestRef::explicit(hmac, period),
        None => ExpireDelegationTokenRequestRef::immediate(hmac),
    };
    expire_delegation_token_request(request, EXPIRE_DELEGATION_TOKEN_OPERATION_BYTES)
        .map_err(|error| admission(preparation_error(error)))
}

const fn preparation_error(
    error: ExpireDelegationTokenRequestFailure,
) -> ExpireDelegationTokenAdmissionErrorKind {
    match error {
        ExpireDelegationTokenRequestFailure::RetainedBytes { .. }
        | ExpireDelegationTokenRequestFailure::Allocation { .. } => {
            ExpireDelegationTokenAdmissionErrorKind::RetainedBytes
        }
        ExpireDelegationTokenRequestFailure::EmptyHmac
        | ExpireDelegationTokenRequestFailure::HmacTooLong { .. }
        | ExpireDelegationTokenRequestFailure::InvalidExpiryTimePeriod { .. } => {
            ExpireDelegationTokenAdmissionErrorKind::InvalidRequest
        }
    }
}

const fn admission(
    kind: ExpireDelegationTokenAdmissionErrorKind,
) -> ExpireDelegationTokenAdmissionError {
    ExpireDelegationTokenAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::ExpireDelegationTokenHostError,
) -> ExpireDelegationTokenAcceptedFaultKind {
    match fault {
        super::ExpireDelegationTokenHostError::Wake => ExpireDelegationTokenAcceptedFaultKind::Wake,
        _ => ExpireDelegationTokenAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpireDelegationTokenAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted ExpireDelegationToken work must retain its observer"]
pub struct ExpireDelegationTokenAccepted {
    observer: ExpireDelegationTokenObserver,
    fault: Option<ExpireDelegationTokenAcceptedFaultKind>,
}

impl ExpireDelegationTokenAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<ExpireDelegationTokenAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> ExpireDelegationTokenObserver {
        self.observer
    }
}

impl fmt::Debug for ExpireDelegationTokenAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpireDelegationTokenAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
