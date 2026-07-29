//! Correlation and lossless normalization of generated resource results.

use kafka_client_core::{
    ConfigAlteration, IncrementalAlterConfigResult, IncrementalAlterConfigsPlan,
    IncrementalConfigResourceAlteration, TopicConfigAlteration,
};
use kafka_wire::{
    IncrementalAlterConfigsResponse,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::response::{
    IncrementalAlterConfigsProtocolFailure, normalize_incremental_alter_configs_response_bounded,
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
        normalize_incremental_alter_configs_response_bounded(&plan(), &response, usize::MAX)
            .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));

    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.topics()[0].topic(), "orders");
    assert!(matches!(
        batch.topics()[0].result(),
        IncrementalAlterConfigResult::Altered
    ));
    let IncrementalAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future broker code"));
    assert!(!error.message_truncated());
}

#[test]
fn malformed_resource_identity_and_negative_throttle_are_rejected() {
    let negative = response(
        -1,
        vec![result(2, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_incremental_alter_configs_response_bounded(&plan(), &negative, usize::MAX,),
        Err(IncrementalAlterConfigsProtocolFailure::ThrottleTime)
    );
    let wrong_type = response(
        0,
        vec![result(4, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_incremental_alter_configs_response_bounded(&plan(), &wrong_type, usize::MAX,),
        Err(IncrementalAlterConfigsProtocolFailure::UnexpectedResource)
    );
    let nonpositive = response(
        0,
        vec![result(0, "orders", 0, None), result(2, "audit", 0, None)],
    );
    assert_eq!(
        normalize_incremental_alter_configs_response_bounded(&plan(), &nonpositive, usize::MAX,),
        Err(IncrementalAlterConfigsProtocolFailure::NonPositiveResourceType)
    );
}

#[test]
fn closed_shape_failures_cover_count_unexpected_missing_and_duplicate_resources() {
    let cases = [
        (
            response(0, vec![result(2, "orders", 0, None)]),
            IncrementalAlterConfigsProtocolFailure::ResourceCount,
        ),
        (
            response(
                0,
                vec![result(2, "orders", 0, None), result(2, "payments", 0, None)],
            ),
            IncrementalAlterConfigsProtocolFailure::UnexpectedResource,
        ),
        (
            response(
                0,
                vec![result(2, "audit", 0, None), result(2, "audit", 0, None)],
            ),
            IncrementalAlterConfigsProtocolFailure::MissingResource,
        ),
        (
            response(
                0,
                vec![result(2, "orders", 0, None), result(2, "orders", 0, None)],
            ),
            IncrementalAlterConfigsProtocolFailure::DuplicateResource,
        ),
    ];
    for (response, expected) in cases {
        assert_eq!(
            normalize_incremental_alter_configs_response_bounded(&plan(), &response, usize::MAX,),
            Err(expected)
        );
    }
}

#[test]
fn generic_response_restores_exact_type_name_order_and_signed_errors() {
    let plan = generic_plan();
    let response = response(
        23,
        vec![
            result(64, "future", 0, None),
            result(32, "group", 0, None),
            result(16, "client", -32_101, Some("future signed code")),
            result(8, "1", 0, None),
            result(4, "1", 0, None),
        ],
    );
    let batch = normalize_incremental_alter_configs_response_bounded(&plan, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("generic response correlates: {error:?}"));

    assert_eq!(batch.throttle_time_ms(), 23);
    assert_eq!(
        batch
            .resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "client"),
            (32, "group"),
            (64, "future"),
        ]
    );
    let IncrementalAlterConfigResult::Failed(error) = batch.resources()[2].result() else {
        panic!("client metrics resource must retain rejection");
    };
    assert_eq!(error.code(), -32_101);
    assert_eq!(error.message(), Some("future signed code"));
}

pub(super) fn plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::new(
        vec![
            TopicConfigAlteration::new(
                "orders".to_owned(),
                vec![ConfigAlteration::delete("retention.ms".to_owned())],
            ),
            TopicConfigAlteration::new(
                "audit".to_owned(),
                vec![ConfigAlteration::set(
                    "cleanup.policy".to_owned(),
                    "delete".to_owned(),
                )],
            ),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid incremental plan: {error}"))
}

fn generic_plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::for_resources(
        [
            (4, "1"),
            (8, "1"),
            (16, "client"),
            (32, "group"),
            (64, "future"),
        ]
        .into_iter()
        .map(|(resource_type, resource_name)| {
            IncrementalConfigResourceAlteration::resource(
                resource_type,
                resource_name.to_owned(),
                vec![ConfigAlteration::delete("key".to_owned())],
            )
        })
        .collect(),
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
) -> IncrementalAlterConfigsResponse {
    let mut response = IncrementalAlterConfigsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.responses = responses;
    response
}
