//! Checked request, correlation-scratch, and result retention accounting.

use core::mem::size_of;

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES,
    ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS, ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES,
    ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS, ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS,
};
use kafka_wire::alter_user_scram_credentials_request::{
    ScramCredentialDeletion, ScramCredentialUpsertion,
};

use super::{
    AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsCorrelationRef,
    AlterUserScramCredentialsRequestRef, NormalizedAlterUserScramCredentialOutcome,
    NormalizedAlterUserScramCredentialsResponse,
    crypto::{GENERATED_SALT_BYTES, output_len},
    request_validation::{CanonicalAlterationKey, FirstUserRef},
};

pub(super) const MAX_ALTERATIONS: usize = ALTER_USER_SCRAM_CREDENTIALS_MAX_CHANGES;
pub(super) const MAX_USERS: usize = ALTER_USER_SCRAM_CREDENTIALS_MAX_USERS;
pub(super) const MAX_USER_BYTES: usize = ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES;
pub(super) const MAX_PASSWORD_BYTES: usize = 16 * 1024;
pub(super) const MIN_SALT_BYTES: usize = 16;
pub(super) const MAX_SALT_BYTES: usize = 64;
pub(super) const MIN_ITERATIONS: u32 = ALTER_USER_SCRAM_CREDENTIALS_MIN_ITERATIONS;
pub(super) const MAX_ITERATIONS: u32 = ALTER_USER_SCRAM_CREDENTIALS_MAX_ITERATIONS;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES;

pub(super) fn request_peak_charge(
    source: AlterUserScramCredentialsRequestRef<'_>,
) -> Option<usize> {
    let scratch = source
        .alterations()
        .len()
        .checked_mul(size_of::<CanonicalAlterationKey<'static>>())?
        .checked_add(
            source
                .alterations()
                .len()
                .checked_mul(size_of::<FirstUserRef<'static>>())?,
        )?;
    source
        .alterations()
        .iter()
        .try_fold(scratch, |bytes, alteration| {
            let bytes = bytes.checked_add(alteration.user().len().checked_mul(2)?)?;
            match alteration {
                AlterUserScramCredentialAlterationRef::Delete { .. } => {
                    bytes.checked_add(size_of::<ScramCredentialDeletion>())
                }
                AlterUserScramCredentialAlterationRef::Upsert {
                    mechanism,
                    password,
                    salt,
                    ..
                } => {
                    let salt_bytes = match salt {
                        Some(salt) => salt.len().checked_mul(2)?,
                        None => GENERATED_SALT_BYTES,
                    };
                    bytes
                        .checked_add(size_of::<ScramCredentialUpsertion>())?
                        .checked_add(password.len())?
                        .checked_add(salt_bytes)?
                        .checked_add(output_len(*mechanism).unwrap_or(0))
                }
            }
        })
}

pub(super) fn response_peak_charge(
    correlation: AlterUserScramCredentialsCorrelationRef<'_>,
    response: &kafka_wire::AlterUserScramCredentialsResponse,
) -> Option<usize> {
    let correlation_charge = correlation
        .affected_users()
        .iter()
        .try_fold(0_usize, |bytes, user| bytes.checked_add(user.len()))?;
    let request_scratch = correlation
        .affected_users()
        .len()
        .checked_mul(size_of::<&'static str>())?;
    let response_scratch = response
        .results
        .len()
        .checked_mul(size_of::<&'static ()>())?;
    let order_scratch = correlation
        .affected_users()
        .len()
        .checked_mul(size_of::<&'static str>())?;
    let normalized = normalized_output_charge(response)?;
    correlation_charge.checked_add(
        request_scratch
            .checked_add(response_scratch)?
            .checked_add(order_scratch)?
            .checked_add(normalized.checked_mul(3)?)?,
    )
}

fn normalized_output_charge(
    response: &kafka_wire::AlterUserScramCredentialsResponse,
) -> Option<usize> {
    response.results.iter().try_fold(
        size_of::<NormalizedAlterUserScramCredentialsResponse>().checked_add(
            response
                .results
                .len()
                .checked_mul(size_of::<NormalizedAlterUserScramCredentialOutcome>())?,
        )?,
        |bytes, result| {
            bytes
                .checked_add(result.user.len())?
                .checked_add(bounded_diagnostic_len(result.error_message.as_deref()))
        },
    )
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedAlterUserScramCredentialsResponse,
) -> Option<usize> {
    response.outcomes.iter().try_fold(
        size_of::<NormalizedAlterUserScramCredentialsResponse>().checked_add(
            response
                .outcomes
                .capacity()
                .checked_mul(size_of::<NormalizedAlterUserScramCredentialOutcome>())?,
        )?,
        |bytes, outcome| {
            bytes
                .checked_add(outcome.user.capacity())?
                .checked_add(outcome.error_message.as_ref().map_or(0, String::capacity))
        },
    )
}

pub(super) fn bounded_diagnostic(message: Option<&str>) -> (Option<&str>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    let len = bounded_diagnostic_len(Some(message));
    (Some(&message[..len]), len < message.len())
}

fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    let mut index = MAX_DIAGNOSTIC_BYTES.min(message.len());
    while !message.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}
