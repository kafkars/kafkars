//! Allocation-free structural validation of generated API-key 50 responses.

use kafka_wire::{
    DescribeUserScramCredentialsResponse,
    describe_user_scram_credentials_response::DescribeUserScramCredentialsResult,
};

use super::{
    DescribeUserScramCredentialsResponseFailure,
    retention::{MAX_CREDENTIAL_INFOS, MAX_CREDENTIALS_PER_USER, MAX_USER_BYTES, MAX_USERS},
    version::supports_describe_user_scram_credentials_version,
};

pub(super) fn validate_response_shape(
    selected_version: i16,
    response: &DescribeUserScramCredentialsResponse,
) -> Result<(), DescribeUserScramCredentialsResponseFailure> {
    if !supports_describe_user_scram_credentials_version(selected_version) {
        return Err(
            DescribeUserScramCredentialsResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    if response.throttle_time_ms < 0 {
        return Err(
            DescribeUserScramCredentialsResponseFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            },
        );
    }
    if response.error_code != 0 && !response.results.is_empty() {
        return Err(
            DescribeUserScramCredentialsResponseFailure::ResultsWithTopLevelError {
                actual: response.results.len(),
            },
        );
    }
    if response.results.len() > MAX_USERS {
        return Err(
            DescribeUserScramCredentialsResponseFailure::TooManyResults {
                actual: response.results.len(),
                max: MAX_USERS,
            },
        );
    }

    let mut credential_count = 0usize;
    for result in &response.results {
        validate_result(result)?;
        credential_count = credential_count
            .checked_add(result.credential_infos.len())
            .unwrap_or(usize::MAX);
        if credential_count > MAX_CREDENTIAL_INFOS {
            return Err(
                DescribeUserScramCredentialsResponseFailure::TooManyCredentialInfos {
                    actual: credential_count,
                    max: MAX_CREDENTIAL_INFOS,
                },
            );
        }
    }
    Ok(())
}

fn validate_result(
    result: &DescribeUserScramCredentialsResult,
) -> Result<(), DescribeUserScramCredentialsResponseFailure> {
    if result.user.is_empty() {
        return Err(DescribeUserScramCredentialsResponseFailure::EmptyUser);
    }
    if result.user.len() > MAX_USER_BYTES {
        return Err(DescribeUserScramCredentialsResponseFailure::UserTooLong {
            actual: result.user.len(),
            max: MAX_USER_BYTES,
        });
    }
    if result.credential_infos.len() > MAX_CREDENTIALS_PER_USER {
        return Err(
            DescribeUserScramCredentialsResponseFailure::TooManyCredentialsForUser {
                actual: result.credential_infos.len(),
                max: MAX_CREDENTIALS_PER_USER,
            },
        );
    }
    if result.error_code == 0 && result.credential_infos.is_empty() {
        return Err(DescribeUserScramCredentialsResponseFailure::EmptyCredentialsOnSuccess);
    }
    if result.error_code != 0 && !result.credential_infos.is_empty() {
        return Err(
            DescribeUserScramCredentialsResponseFailure::CredentialsWithUserError {
                actual: result.credential_infos.len(),
            },
        );
    }

    let mut seen = [false; 128];
    for info in &result.credential_infos {
        if info.mechanism <= 0 {
            return Err(
                DescribeUserScramCredentialsResponseFailure::InvalidMechanism {
                    actual: info.mechanism,
                },
            );
        }
        if info.iterations <= 0 {
            return Err(
                DescribeUserScramCredentialsResponseFailure::NonPositiveIterations {
                    actual: info.iterations,
                },
            );
        }
        let mechanism = info.mechanism as usize;
        if seen[mechanism] {
            return Err(
                DescribeUserScramCredentialsResponseFailure::DuplicateMechanism {
                    actual: info.mechanism,
                },
            );
        }
        seen[mechanism] = true;
    }
    Ok(())
}
