//! Focused evidence for strict caller-order ACL creation result correlation.

use kafka_wire::{CreateAclsResponse, create_acls_response::AclCreationResult};
use kafka_wire_core::StrBytes;

use super::{
    CreateAclsResponseFailure, NormalizedCreateAclResultRef, normalize_create_acls_response,
    retention::{MAX_BINDINGS, MAX_DIAGNOSTIC_BYTES, response_peak_charge},
};

#[test]
fn response_preserves_caller_order_signed_codes_and_nullable_diagnostics() {
    let response = response(vec![
        result(0, None),
        result(-31_999, Some("not authorized")),
        result(42, Some("")),
    ]);

    let mut results = Vec::new();
    let (throttle, retained) =
        normalize_create_acls_response(3, 3, &response, usize::MAX, |result| {
            results.push(result);
            Ok(())
        })
        .expect("valid positional response");
    assert_eq!(throttle, 17);
    assert_eq!(results.len(), 3);
    assert!(retained > 0);

    assert_eq!(results[0].into_parts(), (0, None, false));
    assert_eq!(
        results[1].into_parts(),
        (-31_999, Some("not authorized"), false)
    );
    assert_eq!(results[2].into_parts(), (42, Some(""), false));
}

#[test]
fn visitor_reuses_caller_prepared_result_storage_without_reallocation() {
    let response = response(vec![result(0, None), result(-1, Some("denied"))]);
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(2)
        .expect("caller result capacity");
    let allocation = prepared.as_ptr();
    let capacity = prepared.capacity();

    normalize_create_acls_response(2, 2, &response, usize::MAX, |result| {
        prepared.push(result);
        Ok(())
    })
    .expect("prepared storage");

    assert_eq!(prepared.len(), 2);
    assert_eq!(prepared.capacity(), capacity);
    assert_eq!(prepared.as_ptr(), allocation);
}

#[test]
fn response_truncates_each_diagnostic_at_a_utf8_boundary() {
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let response = response(vec![result(-1, Some(&diagnostic))]);

    let mut normalized = None;
    normalize_create_acls_response(1, 1, &response, usize::MAX, |result| {
        normalized = Some(result);
        Ok(())
    })
    .expect("bounded diagnostic");
    let (code, message, truncated) = normalized.expect("one result").into_parts();
    assert_eq!(code, -1);
    assert_eq!(
        message.as_deref().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(truncated);
}

#[test]
fn response_rejects_unsupported_versions_and_negative_throttle() {
    let response = response(vec![result(0, None)]);
    assert_eq!(
        ignore_response(0, 1, &response, usize::MAX),
        Err(CreateAclsResponseFailure::UnsupportedApiVersion { actual: 0 })
    );
    assert_eq!(
        ignore_response(4, 1, &response, usize::MAX),
        Err(CreateAclsResponseFailure::UnsupportedApiVersion { actual: 4 })
    );

    let mut negative = response;
    negative.throttle_time_ms = -1;
    assert_eq!(
        ignore_response(2, 1, &negative, usize::MAX),
        Err(CreateAclsResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn response_requires_the_exact_admitted_result_count() {
    let response = response(vec![result(0, None)]);
    assert_eq!(
        ignore_response(2, 0, &response, usize::MAX),
        Err(CreateAclsResponseFailure::EmptyExpectedResults)
    );
    assert_eq!(
        ignore_response(2, MAX_BINDINGS + 1, &response, usize::MAX),
        Err(CreateAclsResponseFailure::TooManyExpectedResults {
            actual: MAX_BINDINGS + 1,
            max: MAX_BINDINGS,
        })
    );
    assert_eq!(
        ignore_response(2, 2, &response, usize::MAX),
        Err(CreateAclsResponseFailure::ResultCount {
            expected: 2,
            actual: 1,
        })
    );
}

#[test]
fn complete_normalization_and_terminal_peak_must_fit_before_copying() {
    let response = response(vec![result(-1, Some("first")), result(-2, Some("second"))]);
    let required = response_peak_charge(&response).expect("bounded charge");

    assert_eq!(
        ignore_response(2, 2, &response, required - 1),
        Err(CreateAclsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert_eq!(
        ignore_response(2, 2, &response, required)
            .expect("exact peak")
            .1,
        required
    );
}

#[test]
fn full_shape_is_validated_before_visiting_and_storage_failure_is_explicit() {
    let response = response(vec![result(-1, Some("first"))]);
    let mut visits = 0;
    let malformed = normalize_create_acls_response(2, 2, &response, usize::MAX, |_| {
        visits += 1;
        Ok(())
    });
    assert_eq!(
        malformed,
        Err(CreateAclsResponseFailure::ResultCount {
            expected: 2,
            actual: 1,
        })
    );
    assert_eq!(visits, 0);

    let rejected = normalize_create_acls_response(2, 1, &response, usize::MAX, |_| {
        visits += 1;
        Err(())
    });
    assert_eq!(rejected, Err(CreateAclsResponseFailure::ResultStorage));
    assert_eq!(visits, 1);
}

fn response(results: Vec<AclCreationResult>) -> CreateAclsResponse {
    let mut response = CreateAclsResponse::default();
    response.throttle_time_ms = 17;
    response.results = results;
    response
}

fn result(error_code: i16, error_message: Option<&str>) -> AclCreationResult {
    let mut result = AclCreationResult::default();
    result.error_code = error_code;
    result.error_message = error_message.map(StrBytes::from);
    result
}

fn ignore_response(
    selected_version: i16,
    expected_results: usize,
    response: &CreateAclsResponse,
    retained_limit: usize,
) -> Result<(u32, usize), CreateAclsResponseFailure> {
    normalize_create_acls_response(
        selected_version,
        expected_results,
        response,
        retained_limit,
        |_result: NormalizedCreateAclResultRef<'_>| Ok(()),
    )
}
