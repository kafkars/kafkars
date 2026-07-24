//! Exact reservation and UTF-8-safe diagnostic bounding scenarios.

use kafka_client_core::IncrementalAlterConfigResult;

use super::{
    response::normalize_incremental_alter_configs_response_bounded,
    response_test::{plan, response, result},
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
        normalize_incremental_alter_configs_response_bounded(
            &plan,
            &response,
            required.saturating_sub(1),
        ),
        Err(super::IncrementalAlterConfigsProtocolFailure::RetainedBytes)
    );
    let batch = normalize_incremental_alter_configs_response_bounded(&plan, &response, required)
        .unwrap_or_else(|error| panic!("reserved bounded result: {error:?}"));
    let IncrementalAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
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
fn nullable_diagnostic_stays_absent_without_false_truncation() {
    let plan = plan();
    let response = response(
        0,
        vec![result(2, "orders", -1, None), result(2, "audit", 0, None)],
    );
    let batch = normalize_incremental_alter_configs_response_bounded(&plan, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("nullable diagnostic: {error:?}"));
    let IncrementalAlterConfigResult::Failed(error) = batch.topics()[0].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.message(), None);
    assert!(!error.message_truncated());
}
