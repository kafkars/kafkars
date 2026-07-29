//! Capture-first runtime-neutral admission of one SCRAM credential alteration.

use std::{fmt, time::Duration};

use crate::{
    admin::AdminHandle,
    clock::DeadlineCapture,
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestFailure,
        AlterUserScramCredentialsRequestRef, alter_user_scram_credentials_request,
    },
};

use super::{
    AlterUserScramCredential, AlterUserScramCredentialsAdmissionError,
    AlterUserScramCredentialsAdmissionErrorKind, AlterUserScramCredentialsObserver,
    AlterUserScramCredentialsRequest, host::ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
    model::AlterUserScramCredentialsPlanFailure,
};

impl AdminHandle {
    /// Captures the original public deadline before higher-layer conversion.
    pub fn capture_alter_user_scram_credentials(
        &self,
        timeout: Duration,
    ) -> Result<AlterUserScramCredentialsCapture<'_>, AlterUserScramCredentialsAdmissionError> {
        let deadline = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                AlterUserScramCredentialsAdmissionError::new(
                    AlterUserScramCredentialsAdmissionErrorKind::InvalidDeadline,
                )
            })?;
        Ok(AlterUserScramCredentialsCapture {
            handle: self,
            deadline,
            timeout,
        })
    }

    /// Captures and submits an already engine-owned request.
    pub fn try_alter_user_scram_credentials(
        &self,
        request: AlterUserScramCredentialsRequest,
        timeout: Duration,
    ) -> Result<AlterUserScramCredentialsAccepted, AlterUserScramCredentialsAdmissionError> {
        self.capture_alter_user_scram_credentials(timeout)?
            .try_submit(request)
    }
}

/// Linear original-deadline token bound to one Admin handle.
#[must_use = "dropping abandons the deadline without admitting SCRAM alteration work"]
pub struct AlterUserScramCredentialsCapture<'handle> {
    handle: &'handle AdminHandle,
    deadline: DeadlineCapture,
    timeout: Duration,
}

impl AlterUserScramCredentialsCapture<'_> {
    /// Validates and prepares API key 51 synchronously before atomic admission.
    ///
    /// PBKDF2 and random-salt generation execute on this caller thread. The
    /// plaintext request is dropped and zeroized before the host can own work.
    pub fn try_submit(
        self,
        request: AlterUserScramCredentialsRequest,
    ) -> Result<AlterUserScramCredentialsAccepted, AlterUserScramCredentialsAdmissionError> {
        if self.timeout.is_zero() {
            return Err(admission(
                AlterUserScramCredentialsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let plan = request.plan().map_err(|error| {
            admission(match error {
                AlterUserScramCredentialsPlanFailure::Invalid => {
                    AlterUserScramCredentialsAdmissionErrorKind::InvalidRequest
                }
                AlterUserScramCredentialsPlanFailure::RetainedBytes => {
                    AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes
                }
            })
        })?;
        let refs = request_refs(&request).map_err(admission)?;
        let prepared = alter_user_scram_credentials_request(
            AlterUserScramCredentialsRequestRef::new(&refs),
            ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
        )
        .map_err(|error| admission(preparation_error(error)))?;
        drop(refs);
        drop(request);

        let now = self.handle.clock.now().map_err(|_error| {
            admission(AlterUserScramCredentialsAdmissionErrorKind::HostInvariant)
        })?;
        if self.deadline.deadline().is_elapsed_at(now) {
            return Err(admission(
                AlterUserScramCredentialsAdmissionErrorKind::DeadlineElapsed,
            ));
        }
        let admitted = self
            .handle
            .alter_user_scram_credentials
            .try_admit(now, self.deadline.operation_deadline(), plan, prepared)
            .map_err(admission)?;
        Ok(AlterUserScramCredentialsAccepted {
            observer: admitted.observer,
            fault: admitted.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for AlterUserScramCredentialsCapture<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsCapture")
            .finish_non_exhaustive()
    }
}

fn request_refs(
    request: &AlterUserScramCredentialsRequest,
) -> Result<
    Vec<AlterUserScramCredentialAlterationRef<'_>>,
    AlterUserScramCredentialsAdmissionErrorKind,
> {
    let mut refs = Vec::new();
    refs.try_reserve_exact(request.alterations().len())
        .map_err(|_| AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes)?;
    refs.extend(
        request
            .alterations()
            .iter()
            .map(|alteration| match alteration {
                AlterUserScramCredential::Delete { user, mechanism } => {
                    AlterUserScramCredentialAlterationRef::delete(user, *mechanism)
                }
                AlterUserScramCredential::Upsert {
                    user,
                    mechanism,
                    iterations,
                    password,
                    salt,
                } => AlterUserScramCredentialAlterationRef::upsert(
                    user,
                    *mechanism,
                    *iterations,
                    password,
                    salt.as_deref(),
                ),
            }),
    );
    Ok(refs)
}

const fn preparation_error(
    error: AlterUserScramCredentialsRequestFailure,
) -> AlterUserScramCredentialsAdmissionErrorKind {
    match error {
        AlterUserScramCredentialsRequestFailure::RetainedBytes { .. } => {
            AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes
        }
        AlterUserScramCredentialsRequestFailure::SecureRandom => {
            AlterUserScramCredentialsAdmissionErrorKind::Preparation
        }
        AlterUserScramCredentialsRequestFailure::EmptyAlterations
        | AlterUserScramCredentialsRequestFailure::TooManyAlterations { .. }
        | AlterUserScramCredentialsRequestFailure::TooManyUsers { .. }
        | AlterUserScramCredentialsRequestFailure::EmptyUser
        | AlterUserScramCredentialsRequestFailure::UserTooLong { .. }
        | AlterUserScramCredentialsRequestFailure::UnsupportedMechanism { .. }
        | AlterUserScramCredentialsRequestFailure::IterationsOutOfRange { .. }
        | AlterUserScramCredentialsRequestFailure::EmptyPassword
        | AlterUserScramCredentialsRequestFailure::PasswordTooLong { .. }
        | AlterUserScramCredentialsRequestFailure::SaltTooShort { .. }
        | AlterUserScramCredentialsRequestFailure::SaltTooLong { .. }
        | AlterUserScramCredentialsRequestFailure::DuplicateCredential => {
            AlterUserScramCredentialsAdmissionErrorKind::InvalidRequest
        }
    }
}

const fn admission(
    kind: AlterUserScramCredentialsAdmissionErrorKind,
) -> AlterUserScramCredentialsAdmissionError {
    AlterUserScramCredentialsAdmissionError::new(kind)
}

pub(super) const fn accepted_fault_kind(
    fault: super::AlterUserScramCredentialsHostError,
) -> AlterUserScramCredentialsAcceptedFaultKind {
    match fault {
        super::AlterUserScramCredentialsHostError::Wake => {
            AlterUserScramCredentialsAcceptedFaultKind::Wake
        }
        _ => AlterUserScramCredentialsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterUserScramCredentialsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// A concrete host invariant failed after terminal reservation.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted AlterUserScramCredentials work must retain its observer"]
pub struct AlterUserScramCredentialsAccepted {
    observer: AlterUserScramCredentialsObserver,
    fault: Option<AlterUserScramCredentialsAcceptedFaultKind>,
}

impl AlterUserScramCredentialsAccepted {
    /// Returns post-commit degradation without changing ownership.
    pub const fn fault(&self) -> Option<AlterUserScramCredentialsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> AlterUserScramCredentialsObserver {
        self.observer
    }
}

impl fmt::Debug for AlterUserScramCredentialsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlterUserScramCredentialsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
