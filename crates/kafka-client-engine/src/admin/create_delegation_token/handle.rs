//! Capture-first runtime-neutral admission of one delegation-token creation.

use std::{fmt, time::Duration};

use kafka_client_core::CreateDelegationTokenPlan;

use crate::{
    admin::AdminHandle,
    clock::DeadlineCapture,
    protocol::admin::create_delegation_token::{
        CreateDelegationTokenRequestFailure, CreateDelegationTokenRequestRef,
        DelegationTokenPrincipalRef, create_delegation_token_request,
    },
};

use super::{
    CreateDelegationTokenAdmissionError, CreateDelegationTokenAdmissionErrorKind,
    CreateDelegationTokenObserver, CreateDelegationTokenRequest,
    host::CREATE_DELEGATION_TOKEN_OPERATION_BYTES, model::CreateDelegationTokenPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before request conversion.
    pub fn capture_create_delegation_token(
        &self,
        timeout: Duration,
    ) -> Result<CreateDelegationTokenCapture<'_>, CreateDelegationTokenAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                admission(CreateDelegationTokenAdmissionErrorKind::InvalidDeadline)
            })?;
        Ok(CreateDelegationTokenCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned token request.
    pub fn try_create_delegation_token(
        &self,
        request: CreateDelegationTokenRequest,
        timeout: Duration,
    ) -> Result<CreateDelegationTokenAccepted, CreateDelegationTokenAdmissionError> {
        self.capture_create_delegation_token(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting token-creation work"]
pub struct CreateDelegationTokenCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl CreateDelegationTokenCapture<'_> {
    /// Validates, prepares API key 38, and atomically reserves accepted ownership.
    pub fn try_submit(
        self,
        request: CreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenAccepted, CreateDelegationTokenAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                CreateDelegationTokenAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.into_plan().map_err(|error| {
            admission(match error {
                CreateDelegationTokenPlanFailure::Invalid => {
                    CreateDelegationTokenAdmissionErrorKind::InvalidRequest
                }
                CreateDelegationTokenPlanFailure::RetainedBytes => {
                    CreateDelegationTokenAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let prepared = prepare_request(&plan)?;
        let now =
            self.handle.clock.now().map_err(|_error| {
                admission(CreateDelegationTokenAdmissionErrorKind::HostInvariant)
            })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                CreateDelegationTokenAdmissionErrorKind::DeadlineElapsed,
            ));
        }
        let admitted = self
            .handle
            .create_delegation_token
            .try_admit(now, self.deadline.operation_deadline(), plan, prepared)
            .map_err(admission)?;
        Ok(CreateDelegationTokenAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for CreateDelegationTokenCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateDelegationTokenCapture")
            .finish_non_exhaustive()
    }
}

fn prepare_request(
    plan: &CreateDelegationTokenPlan,
) -> Result<
    crate::protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest,
    CreateDelegationTokenAdmissionError,
> {
    let owner = plan.owner().map(|principal| {
        DelegationTokenPrincipalRef::new(principal.principal_type(), principal.principal_name())
    });
    let mut renewers = Vec::new();
    renewers
        .try_reserve_exact(plan.renewers().len())
        .map_err(|_| admission(CreateDelegationTokenAdmissionErrorKind::RetainedBytes))?;
    renewers.extend(plan.renewers().iter().map(|principal| {
        DelegationTokenPrincipalRef::new(principal.principal_type(), principal.principal_name())
    }));
    let lifetime = plan
        .max_lifetime_ms()
        .map_or(-1, |value| i64::try_from(value).unwrap_or(i64::MAX));
    create_delegation_token_request(
        CreateDelegationTokenRequestRef::new(owner, &renewers, lifetime),
        CREATE_DELEGATION_TOKEN_OPERATION_BYTES,
    )
    .map_err(|error| admission(preparation_error(error)))
}

const fn preparation_error(
    error: CreateDelegationTokenRequestFailure,
) -> CreateDelegationTokenAdmissionErrorKind {
    match error {
        CreateDelegationTokenRequestFailure::RetainedBytes { .. }
        | CreateDelegationTokenRequestFailure::Allocation { .. } => {
            CreateDelegationTokenAdmissionErrorKind::RetainedBytes
        }
        _ => CreateDelegationTokenAdmissionErrorKind::InvalidRequest,
    }
}

const fn admission(
    kind: CreateDelegationTokenAdmissionErrorKind,
) -> CreateDelegationTokenAdmissionError {
    CreateDelegationTokenAdmissionError::new(kind)
}

const fn accepted_fault_kind(
    fault: super::CreateDelegationTokenHostError,
) -> CreateDelegationTokenAcceptedFaultKind {
    match fault {
        super::CreateDelegationTokenHostError::Wake => CreateDelegationTokenAcceptedFaultKind::Wake,
        _ => CreateDelegationTokenAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDelegationTokenAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus advisory post-commit degradation.
#[must_use = "accepted CreateDelegationToken work must retain its observer"]
pub struct CreateDelegationTokenAccepted {
    observer: CreateDelegationTokenObserver,
    fault: Option<CreateDelegationTokenAcceptedFaultKind>,
}

impl CreateDelegationTokenAccepted {
    /// Returns advisory post-commit degradation.
    pub const fn fault(&self) -> Option<CreateDelegationTokenAcceptedFaultKind> {
        self.fault
    }

    /// Consumes acceptance into its named observer.
    pub fn into_observer(self) -> CreateDelegationTokenObserver {
        self.observer
    }
}

impl fmt::Debug for CreateDelegationTokenAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateDelegationTokenAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
