//! Correlation and lossless normalization of generated legacy resource results.

use kafka_client_core::{
    LegacyAlterConfigResult, LegacyAlterConfigsPlan, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};
use kafka_wire::{AlterConfigsResponse, alter_configs_response::AlterConfigsResourceResponse};

use super::response::{
    LegacyAlterConfigsProtocolFailure, normalize_legacy_alter_configs_response_bounded,
};

#[test]
fn response_restores_caller_order_throttle_and_exact_signed_codes() {
    let response = response(
        19,
        vec![
            result(2, "audit", -32_123, Some("future broker code")),
            result(2, "orders", 0, None),
        ],
    );
    let batch =
        normalize_legacy_alter_configs_response_bounded(&plan(), Some(2), &response, usize::MAX)
            .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));

    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.topics()[0].topic(), "orders");
    assert!(matches!(
        batch.topics()[0].result(),
        LegacyAlterConfigResult::Altered
    ));
    let LegacyAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future broker code"));
    assert!(!error.message_truncated());
}

#[test]
fn negative_throttle_and_wrong_resource_type_are_rejected() {
    let negative = response(
        -1,
        vec![result(2, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(&plan(), Some(0), &negative, usize::MAX),
        Err(LegacyAlterConfigsProtocolFailure::ThrottleTime)
    );
    let wrong_type = response(
        0,
        vec![result(4, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(&plan(), Some(1), &wrong_type, usize::MAX,),
        Err(LegacyAlterConfigsProtocolFailure::UnexpectedResource)
    );
    let nonpositive = response(
        0,
        vec![result(0, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(&plan(), Some(1), &nonpositive, usize::MAX,),
        Err(LegacyAlterConfigsProtocolFailure::NonPositiveResourceType)
    );
}

#[test]
fn closed_shape_rejects_count_unexpected_missing_and_duplicate_topics() {
    let cases = [
        (
            response(0, vec![result(2, "orders", 0, None)]),
            LegacyAlterConfigsProtocolFailure::ResourceCount,
        ),
        (
            response(
                0,
                vec![result(2, "orders", 0, None), result(2, "payments", 0, None)],
            ),
            LegacyAlterConfigsProtocolFailure::UnexpectedResource,
        ),
        (
            response(
                0,
                vec![result(2, "audit", 0, None), result(2, "audit", 0, None)],
            ),
            LegacyAlterConfigsProtocolFailure::MissingResource,
        ),
        (
            response(
                0,
                vec![result(2, "orders", 0, None), result(2, "orders", 0, None)],
            ),
            LegacyAlterConfigsProtocolFailure::DuplicateResource,
        ),
    ];
    for (response, expected) in cases {
        assert_eq!(
            normalize_legacy_alter_configs_response_bounded(
                &plan(),
                Some(2),
                &response,
                usize::MAX,
            ),
            Err(expected)
        );
    }
}

#[test]
fn generic_response_correlates_exact_type_name_pairs_in_caller_order() {
    let plan = generic_plan();
    let response = response(
        31,
        vec![
            result(64, "future-resource", -30_001, Some("future")),
            result(16, "payments-client", 0, None),
            result(4, "1", 0, None),
            result(32, "payments-group", -32_123, None),
            result(8, "1", 0, None),
        ],
    );
    let batch =
        normalize_legacy_alter_configs_response_bounded(&plan, Some(2), &response, usize::MAX)
            .unwrap_or_else(|error| panic!("generic response correlates: {error:?}"));

    assert_eq!(batch.throttle_time_ms(), 31);
    assert_eq!(
        batch
            .resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "payments-client"),
            (32, "payments-group"),
            (64, "future-resource"),
        ]
    );
    let LegacyAlterConfigResult::Failed(group_error) = batch.resources()[3].result() else {
        panic!("group broker failure expected");
    };
    assert_eq!(group_error.code(), -32_123);
    let LegacyAlterConfigResult::Failed(future_error) = batch.resources()[4].result() else {
        panic!("future broker failure expected");
    };
    assert_eq!(future_error.code(), -30_001);
}

#[test]
fn generic_response_rejects_wrong_missing_and_duplicate_exact_identities() {
    let plan = LegacyAlterConfigsPlan::for_resources(
        vec![
            LegacyConfigResourceReplacement::resource(4, "1".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(8, "1".to_owned(), Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid exact identity plan: {error}"));
    for (responses, expected) in [
        (
            vec![result(4, "1", 0, None), result(16, "1", 0, None)],
            LegacyAlterConfigsProtocolFailure::UnexpectedResource,
        ),
        (
            vec![result(8, "1", 0, None), result(8, "1", 0, None)],
            LegacyAlterConfigsProtocolFailure::MissingResource,
        ),
        (
            vec![result(4, "1", 0, None), result(4, "1", 0, None)],
            LegacyAlterConfigsProtocolFailure::DuplicateResource,
        ),
    ] {
        assert_eq!(
            normalize_legacy_alter_configs_response_bounded(
                &plan,
                Some(2),
                &response(0, responses),
                usize::MAX,
            ),
            Err(expected)
        );
    }
}

#[test]
fn missing_and_out_of_range_selected_versions_are_rejected_before_binding() {
    let response = response(
        0,
        vec![result(2, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_legacy_alter_configs_response_bounded(&plan(), None, &response, usize::MAX),
        Err(LegacyAlterConfigsProtocolFailure::MissingSelectedVersion)
    );
    for version in [-1, 3] {
        assert_eq!(
            normalize_legacy_alter_configs_response_bounded(
                &plan(),
                Some(version),
                &response,
                usize::MAX,
            ),
            Err(LegacyAlterConfigsProtocolFailure::UnsupportedApiVersion)
        );
    }
}

pub(super) fn plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::new(
        vec![
            LegacyTopicConfigReplacement::new(
                "orders".to_owned(),
                vec![LegacyConfigEntry::new(
                    "cleanup.policy".to_owned(),
                    Some("compact".to_owned()),
                )],
            ),
            LegacyTopicConfigReplacement::new("audit".to_owned(), Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid legacy replacement plan: {error}"))
}

pub(super) fn generic_plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::for_resources(
        vec![
            LegacyConfigResourceReplacement::resource(4, "1".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(8, "1".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(16, "payments-client".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(32, "payments-group".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(64, "future-resource".to_owned(), Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid generic plan: {error}"))
}

pub(super) fn result(
    resource_type: i8,
    topic: &str,
    error_code: i16,
    message: Option<&str>,
) -> AlterConfigsResourceResponse {
    let mut result = AlterConfigsResourceResponse::default();
    result.resource_type = resource_type;
    result.resource_name = topic.into();
    result.error_code = error_code;
    result.error_message = message.map(Into::into);
    result
}

pub(super) fn response(
    throttle_time_ms: i32,
    responses: Vec<AlterConfigsResourceResponse>,
) -> AlterConfigsResponse {
    let mut response = AlterConfigsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.responses = responses;
    response
}
