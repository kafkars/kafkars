//! Checked request, correlation-scratch, and normalized-result accounting.

use core::mem::size_of;

use kafka_wire::{
    DescribeUserScramCredentialsResponse, describe_user_scram_credentials_request::UserName,
};

use super::{
    DescribeUserScramCredentialsRequestRef, NormalizedDescribeUserScramCredentialsResponse,
    NormalizedScramCredentialInfo, NormalizedUserScramCredentials,
};

pub(super) const MAX_USERS: usize = 16 * 1024;
pub(super) const MAX_USER_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_CREDENTIALS_PER_USER: usize = i8::MAX as usize;
pub(super) const MAX_CREDENTIAL_INFOS: usize = MAX_USERS * MAX_CREDENTIALS_PER_USER;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn request_peak_charge(
    source: DescribeUserScramCredentialsRequestRef<'_>,
) -> Option<usize> {
    let users = source.users().unwrap_or_default();
    users.iter().try_fold(
        users
            .len()
            .checked_mul(size_of::<UserName>())?
            .checked_add(users.len().checked_mul(size_of::<&str>())?)?,
        |bytes, user| bytes.checked_add(user.len()),
    )
}

fn generated_request_retained_charge(
    source: DescribeUserScramCredentialsRequestRef<'_>,
) -> Option<usize> {
    let users = source.users().unwrap_or_default();
    users.iter().try_fold(
        users.len().checked_mul(size_of::<UserName>())?,
        |bytes, user| bytes.checked_add(user.len()),
    )
}

pub(super) fn response_peak_charge(
    request: DescribeUserScramCredentialsRequestRef<'_>,
    response: &DescribeUserScramCredentialsResponse,
) -> Option<usize> {
    let request_peak = request_peak_charge(request)?;
    let request_retained = generated_request_retained_charge(request)?;
    let working = response_scratch_charge(request, response)?
        .checked_add(normalized_output_charge(response)?.checked_mul(3)?)?;
    Some(request_peak.max(request_retained.checked_add(working)?))
}

fn response_scratch_charge(
    request: DescribeUserScramCredentialsRequestRef<'_>,
    response: &DescribeUserScramCredentialsResponse,
) -> Option<usize> {
    let request_count = request.users().map_or(0, <[String]>::len);
    let result_count = response.results.len();
    request_count
        .checked_mul(size_of::<&str>())?
        .checked_add(result_count.checked_mul(size_of::<&'static ()>())?)?
        .checked_add(
            request
                .users()
                .is_some()
                .then_some(result_count.checked_mul(size_of::<&'static ()>())?)
                .unwrap_or(0),
        )
}

fn normalized_output_charge(response: &DescribeUserScramCredentialsResponse) -> Option<usize> {
    response.results.iter().try_fold(
        size_of::<NormalizedDescribeUserScramCredentialsResponse>()
            .checked_add(
                response
                    .results
                    .len()
                    .checked_mul(size_of::<NormalizedUserScramCredentials>())?,
            )?
            .checked_add(bounded_diagnostic_len(response.error_message.as_deref()))?,
        |bytes, result| {
            bytes
                .checked_add(result.user.len())?
                .checked_add(bounded_diagnostic_len(result.error_message.as_deref()))?
                .checked_add(
                    result
                        .credential_infos
                        .len()
                        .checked_mul(size_of::<NormalizedScramCredentialInfo>())?,
                )
        },
    )
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedDescribeUserScramCredentialsResponse,
) -> Option<usize> {
    response.results.iter().try_fold(
        size_of::<NormalizedDescribeUserScramCredentialsResponse>()
            .checked_add(
                response
                    .results
                    .capacity()
                    .checked_mul(size_of::<NormalizedUserScramCredentials>())?,
            )?
            .checked_add(response.error_message.as_ref().map_or(0, String::capacity))?,
        |bytes, result| {
            bytes
                .checked_add(result.user.capacity())?
                .checked_add(result.error_message.as_ref().map_or(0, String::capacity))?
                .checked_add(
                    result
                        .credential_infos
                        .capacity()
                        .checked_mul(size_of::<NormalizedScramCredentialInfo>())?,
                )
        },
    )
}

pub(super) fn bounded_diagnostic(message: Option<&str>) -> (Option<&str>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    let end = floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()));
    (Some(&message[..end]), end < message.len())
}

fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    bounded_diagnostic(message).0.map_or(0, str::len)
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}
