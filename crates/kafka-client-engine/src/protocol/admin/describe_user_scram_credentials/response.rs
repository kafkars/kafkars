//! Validate-first bounded normalization of generated SCRAM description facts.

use kafka_wire::{
    DescribeUserScramCredentialsResponse,
    describe_user_scram_credentials_response::DescribeUserScramCredentialsResult,
};

use super::{
    DescribeUserScramCredentialsRequestRef, NormalizedDescribeUserScramCredentialsResponse,
    NormalizedScramCredentialInfo, NormalizedUserScramCredentials,
    correlation::{ordered_results, validate_request_selection},
    retention::{bounded_diagnostic, normalized_retained_charge, response_peak_charge},
    validation::validate_response_shape,
};

/// Malformed response, correlation failure, incompatibility, or exhausted bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeUserScramCredentialsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    ResultsWithTopLevelError { actual: usize },
    TooManyResults { actual: usize, max: usize },
    TooManyCredentialInfos { actual: usize, max: usize },
    EmptyUser,
    UserTooLong { actual: usize, max: usize },
    TooManyCredentialsForUser { actual: usize, max: usize },
    EmptyCredentialsOnSuccess,
    CredentialsWithUserError { actual: usize },
    InvalidMechanism { actual: i8 },
    NonPositiveIterations { actual: i32 },
    DuplicateMechanism { actual: i8 },
    EmptyUserFilter,
    TooManyRequestedUsers { actual: usize, max: usize },
    EmptyRequestedUser,
    RequestedUserTooLong { actual: usize, max: usize },
    DuplicateRequestedUser,
    DuplicateUser,
    MissingUser,
    UnexpectedUser,
    RetainedBytes { required: usize, limit: usize },
}

/// Normalizes v0 without exposing generated DTOs or credential secret material.
pub(crate) fn normalize_describe_user_scram_credentials_response(
    selected_version: i16,
    request: DescribeUserScramCredentialsRequestRef<'_>,
    response: &DescribeUserScramCredentialsResponse,
    retained_limit: usize,
) -> Result<
    NormalizedDescribeUserScramCredentialsResponse,
    DescribeUserScramCredentialsResponseFailure,
> {
    validate_response_shape(selected_version, response)?;
    let required = response_peak_charge(request, response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let ordered = if response.error_code == 0 {
        ordered_results(request, &response.results, required, retained_limit)?
    } else {
        validate_request_selection(request, required, retained_limit)?;
        Vec::new()
    };
    materialize(response, ordered, required, retained_limit)
}

fn materialize(
    response: &DescribeUserScramCredentialsResponse,
    ordered: Vec<&DescribeUserScramCredentialsResult>,
    required: usize,
    limit: usize,
) -> Result<
    NormalizedDescribeUserScramCredentialsResponse,
    DescribeUserScramCredentialsResponseFailure,
> {
    let (error_message, error_message_truncated) =
        copy_diagnostic(response.error_message.as_deref(), required, limit)?;
    let mut results = Vec::new();
    results
        .try_reserve_exact(ordered.len())
        .map_err(|_| retained(required, limit))?;
    for result in ordered {
        results.push(materialize_result(result, required, limit)?);
    }
    let mut normalized = NormalizedDescribeUserScramCredentialsResponse {
        throttle_time_ms: response.throttle_time_ms as u32,
        error_code: response.error_code,
        error_message,
        error_message_truncated,
        results,
        retained_bytes: 0,
    };
    let retained = normalized_retained_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, limit)?;
    normalized.retained_bytes = required.max(retained);
    Ok(normalized)
}

fn materialize_result(
    source: &DescribeUserScramCredentialsResult,
    required: usize,
    limit: usize,
) -> Result<NormalizedUserScramCredentials, DescribeUserScramCredentialsResponseFailure> {
    let (error_message, error_message_truncated) =
        copy_diagnostic(source.error_message.as_deref(), required, limit)?;
    let mut credential_infos = Vec::new();
    credential_infos
        .try_reserve_exact(source.credential_infos.len())
        .map_err(|_| retained(required, limit))?;
    credential_infos.extend(
        source
            .credential_infos
            .iter()
            .map(|info| NormalizedScramCredentialInfo::new(info.mechanism, info.iterations as u32)),
    );
    credential_infos.sort_unstable_by_key(|info| info.into_parts().0);
    Ok(NormalizedUserScramCredentials {
        user: copy_string(source.user.as_str(), required, limit)?,
        error_code: source.error_code,
        error_message,
        error_message_truncated,
        credential_infos,
    })
}

fn copy_diagnostic(
    source: Option<&str>,
    required: usize,
    limit: usize,
) -> Result<(Option<String>, bool), DescribeUserScramCredentialsResponseFailure> {
    let (bounded, truncated) = bounded_diagnostic(source);
    Ok((
        bounded
            .map(|message| copy_string(message, required, limit))
            .transpose()?,
        truncated,
    ))
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeUserScramCredentialsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeUserScramCredentialsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or_else(|| retained(required, limit))
}

const fn retained(required: usize, limit: usize) -> DescribeUserScramCredentialsResponseFailure {
    DescribeUserScramCredentialsResponseFailure::RetainedBytes { required, limit }
}
