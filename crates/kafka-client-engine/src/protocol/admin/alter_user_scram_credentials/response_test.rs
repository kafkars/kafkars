//! Focused exact-code, correlation, hostile-shape, and diagnostic tests.

use kafka_wire::{
    AlterUserScramCredentialsResponse,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};

use super::{
    AlterUserScramCredentialsCorrelationRef, AlterUserScramCredentialsResponseFailure,
    normalize_alter_user_scram_credentials_response,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn response_restores_first_user_order_and_preserves_exact_signed_codes() {
    let users = vec!["zoe".to_owned(), "amy".to_owned()];
    let response = response(
        37,
        vec![
            result("amy", -321, Some("broker-specific")),
            result("zoe", 0, None),
        ],
    );
    let normalized = normalize(&users, &response);
    let (throttle, outcomes, retained) = normalized.into_parts();
    assert_eq!(throttle, 37);
    assert!(retained > 0);
    let first = outcomes[0].clone().into_parts();
    let second = outcomes[1].clone().into_parts();
    assert_eq!(first, ("zoe".to_owned(), 0, None, false));
    let expected = (
        "amy".to_owned(),
        -321,
        Some("broker-specific".to_owned()),
        false,
    );
    assert_eq!(second, expected);
}
#[test]
fn diagnostic_is_utf8_safe_and_bounded_to_one_kibibyte() {
    let users = vec!["alice".to_owned()];
    let mut message = "a".repeat(1023);
    message.push('💣');
    let response = response(0, vec![result("alice", 42, Some(&message))]);
    let normalized = normalize(&users, &response);
    let (_, outcomes, _) = normalized.into_parts();
    let (_, code, diagnostic, truncated) = outcomes[0].clone().into_parts();
    assert_eq!(code, 42);
    assert_eq!(diagnostic.as_ref().map(String::len), Some(1023));
    assert!(truncated);
}
#[test]
fn hostile_top_level_and_result_shapes_are_rejected() {
    let users = vec!["alice".to_owned()];
    let valid = response(0, vec![result("alice", 0, None)]);
    assert_eq!(
        normalize_error(-1, &users, &valid),
        AlterUserScramCredentialsResponseFailure::UnsupportedApiVersion { actual: -1 }
    );
    assert_eq!(
        normalize_error(1, &users, &valid),
        AlterUserScramCredentialsResponseFailure::UnsupportedApiVersion { actual: 1 }
    );
    let negative = response(-1, vec![result("alice", 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &negative),
        AlterUserScramCredentialsResponseFailure::NegativeThrottleTime { actual: -1 }
    );
    let missing = response(0, Vec::new());
    assert_eq!(
        normalize_error(0, &users, &missing),
        AlterUserScramCredentialsResponseFailure::ResultCount {
            expected: 1,
            actual: 0,
        }
    );
    let duplicate = response(0, vec![result("alice", 0, None), result("alice", 0, None)]);
    let two_users = vec!["alice".to_owned(), "bob".to_owned()];
    assert_eq!(
        normalize_error(0, &two_users, &duplicate),
        AlterUserScramCredentialsResponseFailure::DuplicateUser
    );
}
#[test]
fn missing_and_unexpected_user_facts_remain_distinct() {
    let users = vec!["bob".to_owned(), "charlie".to_owned()];
    let unexpected = response(0, vec![result("bob", 0, None), result("adam", 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &unexpected),
        AlterUserScramCredentialsResponseFailure::UnexpectedUser
    );
    let missing = response(0, vec![result("bob", 0, None), result("zulu", 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &missing),
        AlterUserScramCredentialsResponseFailure::MissingUser
    );
}

#[test]
fn non_secret_correlation_rejects_empty_and_duplicate_users() {
    let empty_response = response(0, Vec::new());
    assert_eq!(
        normalize_error(0, &[], &empty_response),
        AlterUserScramCredentialsResponseFailure::EmptyAffectedUsers
    );
    let users = vec!["alice".to_owned(), "alice".to_owned()];
    let duplicate = response(0, vec![result("alice", 0, None), result("bob", 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &duplicate),
        AlterUserScramCredentialsResponseFailure::DuplicateAffectedUser
    );
}

#[test]
fn returned_and_correlation_user_bounds_are_enforced() {
    let users = vec!["alice".to_owned()];
    let empty_user = response(0, vec![result("", 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &empty_user),
        AlterUserScramCredentialsResponseFailure::EmptyUser
    );
    let long_user = "u".repeat(i16::MAX as usize + 1);
    let long_result = response(0, vec![result(&long_user, 0, None)]);
    assert_eq!(
        normalize_error(0, &users, &long_result),
        AlterUserScramCredentialsResponseFailure::UserTooLong {
            actual: long_user.len(),
            max: i16::MAX as usize,
        }
    );
    let empty_correlation = vec![String::new()];
    assert_eq!(
        normalize_error(
            0,
            &empty_correlation,
            &response(0, vec![result("x", 0, None)])
        ),
        AlterUserScramCredentialsResponseFailure::EmptyAffectedUser
    );
    let long_correlation = vec![long_user.clone()];
    assert_eq!(
        normalize_error(
            0,
            &long_correlation,
            &response(0, vec![result("x", 0, None)])
        ),
        AlterUserScramCredentialsResponseFailure::AffectedUserTooLong {
            actual: long_user.len(),
            max: i16::MAX as usize,
        }
    );
}

#[test]
fn hostile_result_count_is_bounded_before_correlation_scratch() {
    let users = vec!["alice".to_owned()];
    let mut results = Vec::new();
    results.resize_with(1025, Default::default);
    let oversized = response(0, results);
    assert_eq!(
        normalize_error(0, &users, &oversized),
        AlterUserScramCredentialsResponseFailure::TooManyResults {
            actual: 1025,
            max: 1024,
        }
    );
}

#[test]
fn exact_retained_limit_is_enforced_before_materialization() {
    let users = vec!["alice".to_owned()];
    let response = response(0, vec![result("alice", 0, Some("diagnostic"))]);
    let normalized = normalize(&users, &response);
    let retained = normalized.into_parts().2;
    let result = normalize_alter_user_scram_credentials_response(
        0,
        AlterUserScramCredentialsCorrelationRef::new(&users),
        &response,
        retained - 1,
    );
    assert!(matches!(
        result,
        Err(AlterUserScramCredentialsResponseFailure::RetainedBytes { .. })
    ));
}

fn normalize(
    users: &[String],
    response: &AlterUserScramCredentialsResponse,
) -> super::NormalizedAlterUserScramCredentialsResponse {
    let result = normalize_alter_user_scram_credentials_response(
        0,
        AlterUserScramCredentialsCorrelationRef::new(users),
        response,
        LIMIT,
    );
    let Ok(normalized) = result else {
        panic!("valid test response must normalize");
    };
    normalized
}

fn normalize_error(
    version: i16,
    users: &[String],
    response: &AlterUserScramCredentialsResponse,
) -> AlterUserScramCredentialsResponseFailure {
    let result = normalize_alter_user_scram_credentials_response(
        version,
        AlterUserScramCredentialsCorrelationRef::new(users),
        response,
        LIMIT,
    );
    let Err(failure) = result else {
        panic!("malformed test response must fail");
    };
    failure
}

fn response(
    throttle_time_ms: i32,
    results: Vec<AlterUserScramCredentialsResult>,
) -> AlterUserScramCredentialsResponse {
    let mut response = AlterUserScramCredentialsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.results = results;
    response
}

fn result(
    user: &str,
    error_code: i16,
    error_message: Option<&str>,
) -> AlterUserScramCredentialsResult {
    let mut result = AlterUserScramCredentialsResult::default();
    result.user = user.into();
    result.error_code = error_code;
    result.error_message = error_message.map(Into::into);
    result
}
