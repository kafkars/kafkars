//! Focused evidence for positional zero-intermediate DeleteAcls normalization.

use kafka_client_core::{DeleteAclFilterResult, DeleteAclMatchResult};
use kafka_wire::delete_acls_response::DeleteAclsMatchingAcl;

use super::{
    DeleteAclsResponseFailure, normalize_delete_acls_response,
    retention::{MAX_DIAGNOSTIC_BYTES, MAX_FILTERS, MAX_MATCHES_PER_FILTER, response_peak_charge},
    test_support::{
        filter_result, matching, normalize, normalize_versioned, prepared_matching,
        prepared_results, response,
    },
};

#[test]
fn response_preserves_filter_and_nested_order_with_exact_signed_errors() {
    let response = response(vec![
        filter_result(
            0,
            None,
            vec![
                matching(0, None, "orders", "User:a", "*", 3),
                matching(-731, Some("denied"), "audit", "User:b", "host", 8),
            ],
        ),
        filter_result(17, Some("filter rejected"), Vec::new()),
    ]);
    let normalized = normalize(&response, 2, usize::MAX).expect("valid response");
    let (throttle, results, retained) = normalized.into_parts();

    assert_eq!(throttle, 23);
    assert!(retained > 0);
    let DeleteAclFilterResult::Matched(bindings) = &results[0] else {
        panic!("first filter must have matches");
    };
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].resource_name(), "orders");
    assert_eq!(bindings[0].operation(), 3);
    assert_eq!(bindings[0].result(), &DeleteAclMatchResult::Deleted);
    assert_eq!(bindings[1].resource_name(), "audit");
    let DeleteAclMatchResult::BrokerFailed(error) = bindings[1].result() else {
        panic!("second binding must retain its broker error");
    };
    assert_eq!(error.code(), -731);
    assert_eq!(error.message(), Some("denied"));
    assert!(!error.message_truncated());

    let DeleteAclFilterResult::BrokerFailed(error) = &results[1] else {
        panic!("second filter must retain its broker error");
    };
    assert_eq!(error.code(), 17);
    assert_eq!(error.message(), Some("filter rejected"));
}

#[test]
fn normalization_reuses_caller_prepared_outer_and_nested_allocations() {
    let response = response(vec![filter_result(
        0,
        None,
        vec![matching(0, None, "orders", "User:a", "*", 3)],
    )]);
    let outer = prepared_results(1);
    let outer_allocation = outer.as_ptr();
    let mut nested_allocation = None;

    let normalized = normalize_delete_acls_response(
        3,
        1,
        &response,
        usize::MAX,
        outer,
        |_filter_index, count| {
            let nested = prepared_matching(count);
            nested_allocation = Some(nested.as_ptr());
            Ok(nested)
        },
    )
    .expect("prepared storage");
    let (_, results, _) = normalized.into_parts();

    assert_eq!(results.as_ptr(), outer_allocation);
    let DeleteAclFilterResult::Matched(bindings) = &results[0] else {
        panic!("matched result");
    };
    assert_eq!(Some(bindings.as_ptr()), nested_allocation);
}

#[test]
fn response_requires_exact_admitted_positions_version_and_throttle() {
    let response = response(vec![filter_result(0, None, Vec::new())]);
    assert_eq!(
        normalize_versioned(0, 1, &response),
        Err(DeleteAclsResponseFailure::UnsupportedApiVersion { actual: 0 })
    );
    assert_eq!(
        normalize_versioned(4, 1, &response),
        Err(DeleteAclsResponseFailure::UnsupportedApiVersion { actual: 4 })
    );
    assert_eq!(
        normalize_versioned(2, 0, &response),
        Err(DeleteAclsResponseFailure::EmptyExpectedFilters)
    );
    assert_eq!(
        normalize_versioned(2, MAX_FILTERS + 1, &response),
        Err(DeleteAclsResponseFailure::TooManyExpectedFilters {
            actual: MAX_FILTERS + 1,
            max: MAX_FILTERS,
        })
    );
    assert_eq!(
        normalize_versioned(2, 2, &response),
        Err(DeleteAclsResponseFailure::FilterResultCount {
            expected: 2,
            actual: 1,
        })
    );

    let mut negative = response;
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize_versioned(2, 1, &negative),
        Err(DeleteAclsResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn response_rejects_filter_errors_with_matches_and_duplicate_bindings() {
    let binding = matching(0, None, "orders", "User:a", "*", 3);
    let malformed = response(vec![filter_result(
        -5,
        Some("failed"),
        vec![binding.clone()],
    )]);
    assert_eq!(
        normalize(&malformed, 1, usize::MAX),
        Err(DeleteAclsResponseFailure::FilterErrorWithMatches {
            filter_index: 0,
            actual: 1,
        })
    );

    let duplicate = response(vec![filter_result(0, None, vec![binding.clone(), binding])]);
    assert_eq!(
        normalize(&duplicate, 1, usize::MAX),
        Err(DeleteAclsResponseFailure::DuplicateMatchingAcl { filter_index: 0 })
    );
}

#[test]
fn response_rejects_invalid_binding_domains_and_nested_caps() {
    let mut invalid = matching(0, None, "orders", "User:a", "*", 3);
    invalid.resource_type = 1;
    let malformed = response(vec![filter_result(0, None, vec![invalid])]);
    assert_eq!(
        normalize(&malformed, 1, usize::MAX),
        Err(DeleteAclsResponseFailure::InvalidResourceType { actual: 1 })
    );

    let hostile = response(vec![filter_result(
        0,
        None,
        vec![DeleteAclsMatchingAcl::default(); MAX_MATCHES_PER_FILTER + 1],
    )]);
    assert_eq!(
        normalize(&hostile, 1, usize::MAX),
        Err(DeleteAclsResponseFailure::TooManyMatchesForFilter {
            filter_index: 0,
            actual: MAX_MATCHES_PER_FILTER + 1,
            max: MAX_MATCHES_PER_FILTER,
        })
    );
}

#[test]
fn diagnostics_are_nullable_utf8_bounded_and_storage_failures_are_explicit() {
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));
    let oversized_response = response(vec![filter_result(-1, Some(&diagnostic), Vec::new())]);
    let normalized = normalize(&oversized_response, 1, usize::MAX).expect("bounded diagnostic");
    let (_, results, _) = normalized.into_parts();
    let DeleteAclFilterResult::BrokerFailed(error) = &results[0] else {
        panic!("broker failure");
    };
    assert_eq!(
        error.message().map(str::len),
        Some(MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(error.message_truncated());

    assert_eq!(
        normalize_delete_acls_response(
            2,
            1,
            &oversized_response,
            usize::MAX,
            Vec::new(),
            |_index, count| Ok(prepared_matching(count)),
        ),
        Err(DeleteAclsResponseFailure::OuterResultStorage)
    );

    let matched = response(vec![filter_result(
        0,
        None,
        vec![matching(0, None, "orders", "User:a", "*", 3)],
    )]);
    assert_eq!(
        normalize_delete_acls_response(
            2,
            1,
            &matched,
            usize::MAX,
            prepared_results(1),
            |_index, _count| Err(()),
        ),
        Err(DeleteAclsResponseFailure::MatchingResultStorage { filter_index: 0 })
    );
}

#[test]
fn validate_first_peak_must_fit_before_terminal_storage_is_requested() {
    let response = response(vec![filter_result(
        0,
        None,
        vec![matching(-7, Some("denied"), "orders", "User:a", "*", 3)],
    )]);
    let required = response_peak_charge(&response).expect("bounded response charge");
    let mut storage_calls = 0;

    let rejected = normalize_delete_acls_response(
        2,
        1,
        &response,
        required - 1,
        prepared_results(1),
        |_index, count| {
            storage_calls += 1;
            Ok(prepared_matching(count))
        },
    );
    assert_eq!(
        rejected,
        Err(DeleteAclsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    assert_eq!(storage_calls, 0);
    assert!(normalize(&response, 1, usize::MAX).is_ok());
}
