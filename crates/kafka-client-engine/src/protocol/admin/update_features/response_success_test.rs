//! Exact error, request-order correlation, v2 synthesis, and diagnostic evidence.

use kafka_wire::{UpdateFeaturesResponse, update_features_response::UpdatableFeatureResult};

use super::{
    NormalizedUpdateFeaturesOutcome, UpdateFeatureMode, UpdateFeatureRef,
    UpdateFeaturesRequestPlan, normalize_update_features_response, retention::DIAGNOSTIC_BYTES,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn v0_v1_results_restore_request_order_and_preserve_signed_errors() {
    let updates = updates();
    let mut response = success_response();
    response.throttle_time_ms = 19;
    response.results = vec![
        result("group.version", 0, None),
        result("metadata.version", -32_000, Some("broker-owned")),
    ];

    let normalized = normalize(1, &response, &updates, LIMIT);
    let (throttle, outcome, retained) = normalized.into_parts();
    let NormalizedUpdateFeaturesOutcome::Results(results) = outcome else {
        panic!("expected ordered results");
    };
    assert_eq!(throttle, 19);
    assert!(retained > 0);
    let (first_feature, first_error) = results[0].clone().into_parts();
    let (second_feature, second_error) = results[1].clone().into_parts();
    assert_eq!(first_feature, "metadata.version");
    assert_eq!(
        first_error
            .unwrap_or_else(|| panic!("first result should fail"))
            .into_parts(),
        (-32_000, Some("broker-owned".to_owned()), false)
    );
    assert_eq!(second_feature, "group.version");
    assert!(second_error.is_none());
}

#[test]
fn v2_top_level_success_synthesizes_success_for_every_requested_feature() {
    let updates = updates();
    let response = success_response();

    let normalized = normalize(2, &response, &updates, LIMIT);
    let (_, outcome, _) = normalized.into_parts();
    let NormalizedUpdateFeaturesOutcome::Results(results) = outcome else {
        panic!("expected synthesized results");
    };
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].clone().into_parts(),
        ("metadata.version".to_owned(), None)
    );
    assert_eq!(
        results[1].clone().into_parts(),
        ("group.version".to_owned(), None)
    );
}

#[test]
fn top_level_error_and_utf8_bounded_result_diagnostic_are_lossless() {
    let updates = updates();
    let mut top = UpdateFeaturesResponse::default();
    top.error_code = -17;
    top.error_message = Some("top-level".into());
    let normalized = normalize(2, &top, &updates, LIMIT);
    let (_, outcome, _) = normalized.into_parts();
    let NormalizedUpdateFeaturesOutcome::TopLevelError(error) = outcome else {
        panic!("expected top-level error");
    };
    assert_eq!(
        error.into_parts(),
        (-17, Some("top-level".to_owned()), false)
    );

    let diagnostic = format!("{}é", "x".repeat(DIAGNOSTIC_BYTES));
    let mut per_feature = success_response();
    per_feature.results = vec![
        result("metadata.version", 7, Some(&diagnostic)),
        result("group.version", 0, None),
    ];
    let normalized = normalize(1, &per_feature, &updates, LIMIT);
    let (_, outcome, _) = normalized.into_parts();
    let NormalizedUpdateFeaturesOutcome::Results(results) = outcome else {
        panic!("expected result batch");
    };
    let (_, error) = results[0].clone().into_parts();
    let (_, message, truncated) = error
        .unwrap_or_else(|| panic!("first result should fail"))
        .into_parts();
    assert!(truncated);
    assert_eq!(
        message.unwrap_or_else(|| panic!("diagnostic")).len(),
        DIAGNOSTIC_BYTES
    );
}

fn updates() -> [UpdateFeatureRef<'static>; 2] {
    [
        UpdateFeatureRef::new("metadata.version", 21, UpdateFeatureMode::Upgrade),
        UpdateFeatureRef::new("group.version", 1, UpdateFeatureMode::SafeDowngrade),
    ]
}

fn success_response() -> UpdateFeaturesResponse {
    let mut response = UpdateFeaturesResponse::default();
    response.error_message = None;
    response
}

fn result(feature: &str, code: i16, message: Option<&str>) -> UpdatableFeatureResult {
    let mut result = UpdatableFeatureResult::default();
    result.feature = feature.into();
    result.error_code = code;
    result.error_message = message.map(Into::into);
    result
}

fn normalize(
    version: i16,
    response: &UpdateFeaturesResponse,
    updates: &[UpdateFeatureRef<'_>],
    limit: usize,
) -> super::NormalizedUpdateFeaturesResponse {
    normalize_update_features_response(
        Some(version),
        response,
        UpdateFeaturesRequestPlan::new(updates, false),
        limit,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"))
}
