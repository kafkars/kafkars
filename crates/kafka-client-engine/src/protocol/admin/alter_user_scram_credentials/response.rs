//! Validate-first bounded normalization of one exact-v0 broker response.

use kafka_wire::{
    AlterUserScramCredentialsResponse,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};

use super::{
    AlterUserScramCredentialsCorrelationRef, NormalizedAlterUserScramCredentialOutcome,
    NormalizedAlterUserScramCredentialsResponse,
    correlation::validate_correlation,
    retention::{
        MAX_USER_BYTES, MAX_USERS, bounded_diagnostic, normalized_retained_charge,
        response_peak_charge,
    },
    version::supports_alter_user_scram_credentials_version,
};

/// Malformed response, failed correlation, or exhausted result capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    TooManyResults { actual: usize, max: usize },
    ResultCount { expected: usize, actual: usize },
    EmptyUser,
    UserTooLong { actual: usize, max: usize },
    EmptyAffectedUsers,
    TooManyAffectedUsers { actual: usize, max: usize },
    EmptyAffectedUser,
    AffectedUserTooLong { actual: usize, max: usize },
    DuplicateAffectedUser,
    DuplicateUser,
    MissingUser,
    UnexpectedUser,
    RetainedBytes { required: usize, limit: usize },
}

/// Preserves exact signed per-user codes and restores first-occurrence order.
pub(crate) fn normalize_alter_user_scram_credentials_response(
    selected_version: i16,
    correlation: AlterUserScramCredentialsCorrelationRef<'_>,
    response: &AlterUserScramCredentialsResponse,
    retained_limit: usize,
) -> Result<NormalizedAlterUserScramCredentialsResponse, AlterUserScramCredentialsResponseFailure> {
    validate_top_level(selected_version, response)?;
    let required = response_peak_charge(correlation, response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_correlation(correlation, required, retained_limit)?;
    if response.results.len() != correlation.affected_users().len() {
        return Err(AlterUserScramCredentialsResponseFailure::ResultCount {
            expected: correlation.affected_users().len(),
            actual: response.results.len(),
        });
    }
    let ordered = correlate(correlation, &response.results, required, retained_limit)?;
    materialize(response, ordered, required, retained_limit)
}

fn validate_top_level(
    selected_version: i16,
    response: &AlterUserScramCredentialsResponse,
) -> Result<(), AlterUserScramCredentialsResponseFailure> {
    if !supports_alter_user_scram_credentials_version(selected_version) {
        return Err(
            AlterUserScramCredentialsResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    if response.throttle_time_ms < 0 {
        return Err(
            AlterUserScramCredentialsResponseFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            },
        );
    }
    if response.results.len() > MAX_USERS {
        return Err(AlterUserScramCredentialsResponseFailure::TooManyResults {
            actual: response.results.len(),
            max: MAX_USERS,
        });
    }
    Ok(())
}

fn correlate<'a>(
    correlation: AlterUserScramCredentialsCorrelationRef<'_>,
    results: &'a [AlterUserScramCredentialsResult],
    required: usize,
    limit: usize,
) -> Result<Vec<&'a AlterUserScramCredentialsResult>, AlterUserScramCredentialsResponseFailure> {
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(results.len())
        .map_err(|_| retained(required, limit))?;
    for result in results {
        validate_result_user(result)?;
        sorted.push(result);
    }
    sorted.sort_unstable_by(|left, right| left.user.as_bytes().cmp(right.user.as_bytes()));
    if sorted.windows(2).any(|pair| pair[0].user == pair[1].user) {
        return Err(AlterUserScramCredentialsResponseFailure::DuplicateUser);
    }
    let expected = correlation.affected_users();
    let mut expected_sorted = Vec::new();
    expected_sorted
        .try_reserve_exact(expected.len())
        .map_err(|_| retained(required, limit))?;
    expected_sorted.extend(expected.iter().map(String::as_str));
    expected_sorted.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    for (expected_user, result) in expected_sorted.iter().zip(&sorted) {
        match result.user.as_bytes().cmp(expected_user.as_bytes()) {
            core::cmp::Ordering::Less => {
                return Err(AlterUserScramCredentialsResponseFailure::UnexpectedUser);
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterUserScramCredentialsResponseFailure::MissingUser);
            }
            core::cmp::Ordering::Equal => {}
        }
    }
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(expected.len())
        .map_err(|_| retained(required, limit))?;
    for expected_user in expected {
        let index = sorted
            .binary_search_by(|result| result.user.as_bytes().cmp(expected_user.as_bytes()))
            .map_err(|_| AlterUserScramCredentialsResponseFailure::MissingUser)?;
        ordered.push(sorted[index]);
    }
    Ok(ordered)
}

fn validate_result_user(
    result: &AlterUserScramCredentialsResult,
) -> Result<(), AlterUserScramCredentialsResponseFailure> {
    if result.user.is_empty() {
        return Err(AlterUserScramCredentialsResponseFailure::EmptyUser);
    }
    if result.user.len() > MAX_USER_BYTES {
        return Err(AlterUserScramCredentialsResponseFailure::UserTooLong {
            actual: result.user.len(),
            max: MAX_USER_BYTES,
        });
    }
    Ok(())
}

fn materialize(
    response: &AlterUserScramCredentialsResponse,
    ordered: Vec<&AlterUserScramCredentialsResult>,
    required: usize,
    limit: usize,
) -> Result<NormalizedAlterUserScramCredentialsResponse, AlterUserScramCredentialsResponseFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        AlterUserScramCredentialsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(ordered.len())
        .map_err(|_| retained(required, limit))?;
    for result in ordered {
        let (message, truncated) = bounded_diagnostic(result.error_message.as_deref());
        outcomes.push(NormalizedAlterUserScramCredentialOutcome {
            user: copy_string(result.user.as_str(), required, limit)?,
            error_code: result.error_code,
            error_message: message
                .map(|message| copy_string(message, required, limit))
                .transpose()?,
            error_message_truncated: truncated,
        });
    }
    let mut normalized = NormalizedAlterUserScramCredentialsResponse {
        throttle_time_ms,
        outcomes,
        retained_bytes: 0,
    };
    let retained = normalized_retained_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, limit)?;
    normalized.retained_bytes = required.max(retained);
    Ok(normalized)
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, AlterUserScramCredentialsResponseFailure> {
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
) -> Result<(), AlterUserScramCredentialsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or_else(|| retained(required, limit))
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsResponseFailure {
    AlterUserScramCredentialsResponseFailure::RetainedBytes { required, limit }
}
