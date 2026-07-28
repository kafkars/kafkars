//! Hostile-shape rejection evidence for SCRAM description responses.

use super::{
    DescribeUserScramCredentialsRequestRef, DescribeUserScramCredentialsResponseFailure,
    normalize_describe_user_scram_credentials_response,
    response_test::{described, failed, response},
    retention::{MAX_CREDENTIALS_PER_USER, MAX_USER_BYTES, MAX_USERS},
};

#[test]
fn filtered_correlation_rejects_duplicate_missing_and_extra_users() {
    let users = vec!["alice".to_owned(), "bob".to_owned()];
    let request = DescribeUserScramCredentialsRequestRef::selected(&users);

    let duplicate = response(vec![
        described("alice", &[(1, 4096)]),
        described("alice", &[(2, 8192)]),
    ]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(0, request, &duplicate, usize::MAX,),
        Err(DescribeUserScramCredentialsResponseFailure::DuplicateUser)
    );

    let missing = response(vec![described("alice", &[(1, 4096)])]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(0, request, &missing, usize::MAX),
        Err(DescribeUserScramCredentialsResponseFailure::MissingUser)
    );

    let extra = response(vec![
        described("alice", &[(1, 4096)]),
        described("bob", &[(2, 8192)]),
        described("mallory", &[(7, 16384)]),
    ]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(0, request, &extra, usize::MAX),
        Err(DescribeUserScramCredentialsResponseFailure::UnexpectedUser)
    );
}

#[test]
fn invalid_mechanisms_iterations_and_duplicates_are_rejected() {
    for (credentials, expected) in [
        (
            vec![(0, 4096)],
            DescribeUserScramCredentialsResponseFailure::InvalidMechanism { actual: 0 },
        ),
        (
            vec![(-1, 4096)],
            DescribeUserScramCredentialsResponseFailure::InvalidMechanism { actual: -1 },
        ),
        (
            vec![(1, 0)],
            DescribeUserScramCredentialsResponseFailure::NonPositiveIterations { actual: 0 },
        ),
        (
            vec![(1, -1)],
            DescribeUserScramCredentialsResponseFailure::NonPositiveIterations { actual: -1 },
        ),
        (
            vec![(1, 4096), (1, 8192)],
            DescribeUserScramCredentialsResponseFailure::DuplicateMechanism { actual: 1 },
        ),
    ] {
        let response = response(vec![described("alice", &credentials)]);
        assert_eq!(
            normalize_describe_user_scram_credentials_response(
                0,
                DescribeUserScramCredentialsRequestRef::all(),
                &response,
                usize::MAX,
            ),
            Err(expected)
        );
    }
}

#[test]
fn hostile_result_counts_user_lengths_and_credential_counts_are_rejected() {
    let too_many_results = response(vec![failed("alice", -1, None); MAX_USERS + 1]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &too_many_results,
            usize::MAX,
        ),
        Err(
            DescribeUserScramCredentialsResponseFailure::TooManyResults {
                actual: MAX_USERS + 1,
                max: MAX_USERS,
            }
        )
    );

    let oversized_user = response(vec![described(
        &"x".repeat(MAX_USER_BYTES + 1),
        &[(1, 4096)],
    )]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &oversized_user,
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::UserTooLong {
            actual: MAX_USER_BYTES + 1,
            max: MAX_USER_BYTES,
        })
    );

    let credentials = vec![(1, 4096); MAX_CREDENTIALS_PER_USER + 1];
    let too_many_credentials = response(vec![described("alice", &credentials)]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &too_many_credentials,
            usize::MAX,
        ),
        Err(
            DescribeUserScramCredentialsResponseFailure::TooManyCredentialsForUser {
                actual: MAX_CREDENTIALS_PER_USER + 1,
                max: MAX_CREDENTIALS_PER_USER,
            }
        )
    );
}

#[test]
fn success_and_error_payloads_cannot_be_conflated() {
    let empty_success = response(vec![described("alice", &[])]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &empty_success,
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::EmptyCredentialsOnSuccess)
    );

    let mut errored = failed("alice", -19, None);
    errored.credential_infos = described("alice", &[(1, 4096)]).credential_infos;
    let credentials_with_error = response(vec![errored]);
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &credentials_with_error,
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::CredentialsWithUserError { actual: 1 })
    );
}

#[test]
fn top_level_shape_version_and_throttle_are_validated_first() {
    let mut with_results = response(vec![described("alice", &[(1, 4096)])]);
    with_results.error_code = -29;
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &with_results,
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::ResultsWithTopLevelError { actual: 1 })
    );

    let mut negative_throttle = response(Vec::new());
    negative_throttle.throttle_time_ms = -1;
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            0,
            DescribeUserScramCredentialsRequestRef::all(),
            &negative_throttle,
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::NegativeThrottleTime { actual: -1 })
    );
    assert_eq!(
        normalize_describe_user_scram_credentials_response(
            1,
            DescribeUserScramCredentialsRequestRef::all(),
            &response(Vec::new()),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsResponseFailure::UnsupportedApiVersion { actual: 1 })
    );
}
