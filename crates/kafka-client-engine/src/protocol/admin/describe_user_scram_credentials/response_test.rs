//! Correlation, ordering, and exact-fact normalization evidence for API-key 50.

use kafka_wire::{
    DescribeUserScramCredentialsResponse,
    describe_user_scram_credentials_response::{
        CredentialInfo, DescribeUserScramCredentialsResult,
    },
};

use super::{
    DescribeUserScramCredentialsRequestRef, normalize_describe_user_scram_credentials_response,
};

#[test]
fn filtered_results_restore_caller_order_and_preserve_exact_facts() {
    let users = vec!["zoë".to_owned(), "missing".to_owned(), "alice".to_owned()];
    let mut response = response(vec![
        described("alice", &[(7, 8192)]),
        failed("missing", -19, Some("not found")),
        described("zoë", &[(2, 8192), (1, 4096)]),
    ]);
    response.throttle_time_ms = 13;
    let normalized = normalize_describe_user_scram_credentials_response(
        0,
        DescribeUserScramCredentialsRequestRef::selected(&users),
        &response,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (throttle, top_code, _, _, results, retained) = normalized.into_parts();
    assert_eq!(throttle, 13);
    assert_eq!(top_code, 0);
    assert!(retained > 0);

    let identities: Vec<_> = results.iter().map(|result| result.user.as_str()).collect();
    assert_eq!(identities, vec!["zoë", "missing", "alice"]);
    assert_eq!(
        results[0]
            .credential_infos
            .iter()
            .map(|info| info.into_parts().0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let (_, code, message, truncated, infos) = results
        .into_iter()
        .nth(1)
        .unwrap_or_else(|| panic!("missing second result"))
        .into_parts();
    assert_eq!(code, -19);
    assert_eq!(message.as_deref(), Some("not found"));
    assert!(!truncated);
    assert!(infos.is_empty());
}

#[test]
fn unfiltered_results_use_utf8_byte_order_and_keep_future_positive_mechanisms() {
    let response = response(vec![
        described("zoë", &[(127, 8192)]),
        described("alice", &[(1, 4096)]),
        described("ábaco", &[(2, 8192)]),
    ]);
    let normalized = normalize_describe_user_scram_credentials_response(
        0,
        DescribeUserScramCredentialsRequestRef::all(),
        &response,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid all-user response: {error:?}"));
    let (_, _, _, _, results, _) = normalized.into_parts();
    let identities: Vec<_> = results.iter().map(|result| result.user.as_str()).collect();
    assert_eq!(identities, vec!["alice", "zoë", "ábaco"]);
    let (_, iterations) = results[1].credential_infos[0].into_parts();
    assert_eq!(iterations, 8192);
    assert_eq!(results[1].credential_infos[0].into_parts().0, 127);
}

#[test]
fn top_level_error_and_diagnostics_remain_exact_and_utf8_safe() {
    let users = vec!["alice".to_owned()];
    let mut response = response(Vec::new());
    response.error_code = -41;
    response.error_message = Some(format!("{}é", "x".repeat(1023)).as_str().into());
    let normalized = normalize_describe_user_scram_credentials_response(
        0,
        DescribeUserScramCredentialsRequestRef::selected(&users),
        &response,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid top-level error: {error:?}"));
    let (_, code, message, truncated, results, _) = normalized.into_parts();
    assert_eq!(code, -41);
    assert_eq!(
        message
            .as_deref()
            .unwrap_or_else(|| panic!("expected diagnostic"))
            .len(),
        1023
    );
    assert!(truncated);
    assert!(results.is_empty());
}

#[test]
fn per_user_diagnostics_are_bounded_on_a_utf8_boundary() {
    let mut response = response(vec![failed(
        "alice",
        -19,
        Some(format!("{}é", "x".repeat(1023)).as_str()),
    )]);
    response.error_message = None;
    let normalized = normalize_describe_user_scram_credentials_response(
        0,
        DescribeUserScramCredentialsRequestRef::all(),
        &response,
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid user error: {error:?}"));
    let (_, _, _, _, mut results, _) = normalized.into_parts();
    let (_, code, message, truncated, infos) = results
        .pop()
        .unwrap_or_else(|| panic!("expected user result"))
        .into_parts();
    assert_eq!(code, -19);
    assert_eq!(message.as_deref().map(str::len), Some(1023));
    assert!(truncated);
    assert!(infos.is_empty());
}

pub(super) fn response(
    results: Vec<DescribeUserScramCredentialsResult>,
) -> DescribeUserScramCredentialsResponse {
    let mut response = DescribeUserScramCredentialsResponse::default();
    response.error_message = None;
    response.results = results;
    response
}

pub(super) fn described(
    user: &str,
    credentials: &[(i8, i32)],
) -> DescribeUserScramCredentialsResult {
    let mut result = DescribeUserScramCredentialsResult::default();
    result.user = user.into();
    result.error_message = None;
    result.credential_infos = credentials
        .iter()
        .map(|(mechanism, iterations)| {
            let mut info = CredentialInfo::default();
            info.mechanism = *mechanism;
            info.iterations = *iterations;
            info
        })
        .collect();
    result
}

pub(super) fn failed(
    user: &str,
    error_code: i16,
    message: Option<&str>,
) -> DescribeUserScramCredentialsResult {
    let mut result = DescribeUserScramCredentialsResult::default();
    result.user = user.into();
    result.error_code = error_code;
    result.error_message = message.map(Into::into);
    result.credential_infos = Vec::new();
    result
}
