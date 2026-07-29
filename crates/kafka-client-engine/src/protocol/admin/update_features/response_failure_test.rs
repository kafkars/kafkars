//! Version, scalar, contradiction, correlation, and retained-capacity rejection.

use kafka_wire::{UpdateFeaturesResponse, update_features_response::UpdatableFeatureResult};

use super::{
    UpdateFeatureMode, UpdateFeatureRef, UpdateFeaturesProtocolFailure, UpdateFeaturesRequestPlan,
    normalize_update_features_response,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn selected_version_and_throttle_are_strict() {
    let updates = updates();
    let response = success_response();
    assert_eq!(
        normalize(None, &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize(Some(3), &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::UnsupportedApiVersion { actual: 3 })
    );
    let mut response = response;
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize(Some(2), &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn top_level_and_version_specific_response_contradictions_are_rejected() {
    let updates = updates();
    let mut broker_error = UpdateFeaturesResponse::default();
    broker_error.error_code = 7;
    broker_error.results = vec![result("feature", 0, None)];
    assert_eq!(
        normalize(Some(1), &broker_error, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::TopLevelErrorWithResults)
    );

    let mut success_diagnostic = success_response();
    success_diagnostic.error_message = Some("contradiction".into());
    assert_eq!(
        normalize(Some(2), &success_diagnostic, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::SuccessDiagnostic {
            field: "top_level_error_message"
        })
    );

    let mut v2_results = success_response();
    v2_results.results = vec![result("feature", 0, None)];
    assert_eq!(
        normalize(Some(2), &v2_results, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::V2ResultsPresent)
    );
}

#[test]
fn v0_v1_results_must_correlate_exactly_once_to_every_request() {
    let updates = updates();
    let mut response = success_response();
    assert!(matches!(
        normalize(Some(1), &response, &updates, LIMIT),
        Err(UpdateFeaturesProtocolFailure::ResultCount { .. })
    ));

    response.results = vec![result("feature", 0, None), result("unexpected", 0, None)];
    assert_eq!(
        normalize(Some(1), &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::UnexpectedFeature)
    );

    response.results = vec![result("feature", 0, None), result("feature", 0, None)];
    assert_eq!(
        normalize(Some(1), &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::DuplicateFeature)
    );

    response.results = vec![
        result("feature", 0, Some("contradiction")),
        result("other", 0, None),
    ];
    assert_eq!(
        normalize(Some(1), &response, &updates, LIMIT).err(),
        Some(UpdateFeaturesProtocolFailure::SuccessDiagnostic {
            field: "result_error_message"
        })
    );
}

#[test]
fn normalized_result_must_fit_the_admitted_retained_capacity() {
    let updates = updates();
    let response = success_response();
    assert!(matches!(
        normalize(Some(2), &response, &updates, 0),
        Err(UpdateFeaturesProtocolFailure::RetainedBytes { .. })
    ));
}

fn updates() -> [UpdateFeatureRef<'static>; 2] {
    [
        UpdateFeatureRef::new("feature", 1, UpdateFeatureMode::Upgrade),
        UpdateFeatureRef::new("other", 2, UpdateFeatureMode::Upgrade),
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
    version: Option<i16>,
    response: &UpdateFeaturesResponse,
    updates: &[UpdateFeatureRef<'_>],
    limit: usize,
) -> Result<super::NormalizedUpdateFeaturesResponse, UpdateFeaturesProtocolFailure> {
    normalize_update_features_response(
        version,
        response,
        UpdateFeaturesRequestPlan::new(updates, false),
        limit,
    )
}
