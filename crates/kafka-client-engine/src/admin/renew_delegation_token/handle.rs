//! Capture-first runtime-neutral admission of one delegation-token renewal.

use std::{fmt, time::Duration};

use kafka_client_core::RenewDelegationTokenPlan;

use crate::{
    admin::AdminHandle,
    clock::DeadlineCapture,
    protocol::admin::renew_delegation_token::{
        RenewDelegationTokenRequestFailure, RenewDelegationTokenRequestRef,
        renew_delegation_token_request,
    },
};

use super::{
    RenewDelegationTokenAdmissionError, RenewDelegationTokenAdmissionErrorKind,
    RenewDelegationTokenObserver, RenewDelegationTokenRequest,
    host::RENEW_DELEGATION_TOKEN_OPERATION_BYTES, model::RenewDelegationTokenPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before secret request conversion.
    pub fn capture_renew_delegation_token(
        &self,
        timeout: Duration,
    ) -> Result<RenewDelegationTokenCapture<'_>, RenewDelegationTokenAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| admission(RenewDelegationTokenAdmissionErrorKind::InvalidDeadline))?;
        Ok(RenewDelegationTokenCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned token renewal.
    pub fn try_renew_delegation_token(
        &self,
        request: RenewDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<RenewDelegationTokenAccepted, RenewDelegationTokenAdmissionError> {
        self.capture_renew_delegation_token(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting token-renewal work"]
pub struct RenewDelegationTokenCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl RenewDelegationTokenCapture<'_> {
    /// Validates, prepares API key 39, and atomically reserves accepted ownership.
    pub fn try_submit(
        self,
        request: RenewDelegationTokenRequest,
    ) -> Result<RenewDelegationTokenAccepted, RenewDelegationTokenAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                RenewDelegationTokenAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.into_plan().map_err(|error| {
            admission(match error {
                RenewDelegationTokenPlanFailure::Invalid => {
                    RenewDelegationTokenAdmissionErrorKind::InvalidRequest
                }
                RenewDelegationTokenPlanFailure::RetainedBytes => {
                    RenewDelegationTokenAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let prepared = prepare_request(&plan)?;
        let now =
            self.handle.clock.now().map_err(|_error| {
                admission(RenewDelegationTokenAdmissionErrorKind::HostInvariant)
            })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                RenewDelegationTokenAdmissionErrorKind::DeadlineElapsed,
            ));
        }
        let admitted = self
            .handle
            .renew_delegation_token
            .try_admit(now, self.deadline.operation_deadline(), plan, prepared)
            .map_err(admission)?;
        Ok(RenewDelegationTokenAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for RenewDelegationTokenCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenCapture")
            .finish_non_exhaustive()
    }
}

fn prepare_request(
    plan: &RenewDelegationTokenPlan,
) -> Result<
    crate::protocol::admin::renew_delegation_token::PreparedRenewDelegationTokenRequest,
    RenewDelegationTokenAdmissionError,
> {
    let hmac = plan.hmac().as_bytes();
    let request = match plan.renew_period_ms() {
        Some(period) => RenewDelegationTokenRequestRef::explicit(hmac, period),
        None => RenewDelegationTokenRequestRef::broker_default(hmac),
    };
    renew_delegation_token_request(request, RENEW_DELEGATION_TOKEN_OPERATION_BYTES)
        .map_err(|error| admission(preparation_error(error)))
}

const fn preparation_error(
    error: RenewDelegationTokenRequestFailure,
) -> RenewDelegationTokenAdmissionErrorKind {
    match error {
        RenewDelegationTokenRequestFailure::RetainedBytes { .. }
        | RenewDelegationTokenRequestFailure::Allocation { .. } => {
            RenewDelegationTokenAdmissionErrorKind::RetainedBytes
        }
        RenewDelegationTokenRequestFailure::EmptyHmac
        | RenewDelegationTokenRequestFailure::HmacTooLong { .. }
        | RenewDelegationTokenRequestFailure::InvalidRenewPeriod { .. } => {
            RenewDelegationTokenAdmissionErrorKind::InvalidRequest
        }
    }
}

const fn admission(
    kind: RenewDelegationTokenAdmissionErrorKind,
) -> RenewDelegationTokenAdmissionError {
    RenewDelegationTokenAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::RenewDelegationTokenHostError,
) -> RenewDelegationTokenAcceptedFaultKind {
    match fault {
        super::RenewDelegationTokenHostError::Wake => RenewDelegationTokenAcceptedFaultKind::Wake,
        _ => RenewDelegationTokenAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenewDelegationTokenAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted RenewDelegationToken work must retain its observer"]
pub struct RenewDelegationTokenAccepted {
    observer: RenewDelegationTokenObserver,
    fault: Option<RenewDelegationTokenAcceptedFaultKind>,
}

impl RenewDelegationTokenAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<RenewDelegationTokenAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> RenewDelegationTokenObserver {
        self.observer
    }
}

impl fmt::Debug for RenewDelegationTokenAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenewDelegationTokenAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
