//! Exact reservation and UTF-8-safe diagnostic bounding scenarios.

use kafka_client_core::LegacyAlterConfigResult;

use super::{
    response::normalize_legacy_alter_configs_response_bounded,
    response_test::{generic_plan, plan, response, result},
    retention::required_result_reservation,
};
use crate::admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC;

#[test]
fn retained_budget_bounds_utf8_diagnostics_before_core_ownership() {
    let plan = plan();
    let required = required_result_reservation(&plan)
        .unwrap_or_else(|error| panic!("small result reservation: {error:?}"));
    let diagnostic = "é".repeat(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
    let response = response(
        0,
        vec![
            result(2, "orders", 0, None),
            result(2, "audit", -1, Some(&diagnostic)),
        ],
    );

    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(
            &plan,
            Some(2),
            &response,
            required.saturating_sub(1),
        ),
        Err(super::LegacyAlterConfigsProtocolFailure::RetainedBytes)
    );
    let batch =
        normalize_legacy_alter_configs_response_bounded(&plan, Some(2), &response, required)
            .unwrap_or_else(|error| panic!("reserved bounded result: {error:?}"));
    let LegacyAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
        panic!("broker failure expected");
    };
    assert!(error.message_truncated());
    assert_eq!(
        error.message().map(str::len),
        Some(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
    );
    assert!(diagnostic.starts_with(error.message().unwrap_or_default()));
}

#[test]
fn generic_resource_identity_and_diagnostics_are_covered_by_result_reservation() {
    let plan = generic_plan();
    let required = required_result_reservation(&plan)
        .unwrap_or_else(|error| panic!("generic result reservation: {error:?}"));
    let response = response(
        0,
        vec![
            result(4, "1", 0, None),
            result(8, "1", 0, None),
            result(16, "payments-client", 0, None),
            result(32, "payments-group", 0, None),
            result(64, "future-resource", -30_001, Some("future")),
        ],
    );

    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(
            &plan,
            Some(2),
            &response,
            required.saturating_sub(1),
        ),
        Err(super::LegacyAlterConfigsProtocolFailure::RetainedBytes)
    );
    assert!(
        normalize_legacy_alter_configs_response_bounded(&plan, Some(2), &response, required)
            .is_ok()
    );
}

#[test]
fn nullable_diagnostic_stays_absent_without_false_truncation() {
    let plan = plan();
    let response = response(
        0,
        vec![result(2, "orders", -1, None), result(2, "audit", 0, None)],
    );
    let batch =
        normalize_legacy_alter_configs_response_bounded(&plan, Some(0), &response, usize::MAX)
            .unwrap_or_else(|error| panic!("nullable diagnostic: {error:?}"));
    let LegacyAlterConfigResult::Failed(error) = batch.topics()[0].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.message(), None);
    assert!(!error.message_truncated());
}
