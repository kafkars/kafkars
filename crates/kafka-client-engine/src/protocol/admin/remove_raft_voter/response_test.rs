//! Version, scalar, diagnostic, and retained-capacity normalization evidence.

use kafka_wire::RemoveRaftVoterResponse;
use kafka_wire_core::StrBytes;

use super::{
    REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES, RemoveRaftVoterResponseFailure,
    normalize_remove_raft_voter_response, retention::REMOVE_RAFT_VOTER_MAX_DIAGNOSTIC_BYTES,
};

#[test]
fn response_preserves_success_and_nullable_diagnostic() {
    let response = response(9, 0, None);
    let normalized = normalize_remove_raft_voter_response(
        Some(0),
        &response,
        REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES,
    )
    .expect("valid success");
    let retained = core::mem::size_of_val(&normalized);
    assert_eq!(normalized.into_parts(), (9, 0, None, false, retained));
}

#[test]
fn response_preserves_signed_code_and_utf8_safe_bounded_diagnostic() {
    let diagnostic = format!(
        "{}é",
        "x".repeat(REMOVE_RAFT_VOTER_MAX_DIAGNOSTIC_BYTES - 1)
    );
    let response = response(3, -32_000, Some(&diagnostic));
    let normalized = normalize_remove_raft_voter_response(
        Some(0),
        &response,
        REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES,
    )
    .expect("valid broker rejection");
    let (throttle, code, message, truncated, retained) = normalized.into_parts();

    assert_eq!(throttle, 3);
    assert_eq!(code, -32_000);
    assert_eq!(
        message.as_deref().map(str::len),
        Some(REMOVE_RAFT_VOTER_MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(truncated);
    assert!(retained <= REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES);
}

#[test]
fn response_rejects_missing_wrong_version_negative_throttle_and_small_limit() {
    let valid = response(0, 0, Some("denied"));
    assert_eq!(
        normalize_remove_raft_voter_response(None, &valid, REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES),
        Err(RemoveRaftVoterResponseFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_remove_raft_voter_response(Some(1), &valid, REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES),
        Err(RemoveRaftVoterResponseFailure::UnsupportedApiVersion { actual: 1 })
    );
    let negative = response(-1, 0, None);
    assert_eq!(
        normalize_remove_raft_voter_response(
            Some(0),
            &negative,
            REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES
        ),
        Err(RemoveRaftVoterResponseFailure::NegativeThrottleTime { actual: -1 })
    );
    assert!(matches!(
        normalize_remove_raft_voter_response(Some(0), &valid, 1),
        Err(RemoveRaftVoterResponseFailure::RetainedBytes { limit: 1, .. })
    ));
}

fn response(
    throttle_time_ms: i32,
    error_code: i16,
    diagnostic: Option<&str>,
) -> RemoveRaftVoterResponse {
    let mut response = RemoveRaftVoterResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response.error_message = diagnostic.map(StrBytes::from);
    response
}
