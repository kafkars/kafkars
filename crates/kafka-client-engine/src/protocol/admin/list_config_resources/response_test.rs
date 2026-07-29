//! Selected-version, error isolation, validation, ordering, and capacity evidence.

use kafka_wire::{ListConfigResourcesResponse, list_config_resources_response::ConfigResource};

use super::{
    ListConfigResourcesProtocolFailure, normalize_list_config_resources_response,
    retention::MAX_NORMALIZED_BYTES,
};

fn resource(resource_type: i8, name: &str) -> ConfigResource {
    let mut resource = ConfigResource::default();
    resource.resource_type = resource_type;
    resource.resource_name = name.into();
    resource
}

#[test]
fn selected_v1_success_is_canonical_by_type_then_name_bytes() {
    let mut response = ListConfigResourcesResponse::default();
    response.throttle_time_ms = 12;
    response.config_resources = vec![
        resource(16, "z"),
        resource(2, "beta"),
        resource(2, "alpha"),
        resource(17, "future"),
    ];
    let facts =
        normalize_list_config_resources_response(Some(1), &response, MAX_NORMALIZED_BYTES).unwrap();
    assert_eq!(facts.throttle_time_ms(), 12);
    assert_eq!(facts.broker_error_code(), 0);
    let resources = facts
        .resources()
        .iter()
        .map(|resource| (resource.resource_type(), resource.resource_name()))
        .collect::<Vec<_>>();
    assert_eq!(
        resources,
        [(2, "alpha"), (2, "beta"), (16, "z"), (17, "future")]
    );
    assert!(facts.retained_bytes() > 0);
}

#[test]
fn broker_error_preserves_signed_code_without_binding_payload() {
    let mut response = ListConfigResourcesResponse::default();
    response.error_code = -7;
    response.config_resources = vec![resource(0, "")];
    let facts = normalize_list_config_resources_response(Some(1), &response, 128).unwrap();
    assert_eq!(facts.broker_error_code(), -7);
    assert!(facts.resources().is_empty());
}

#[test]
fn selected_version_throttle_shape_duplicate_and_capacity_are_strict() {
    let response = ListConfigResourcesResponse::default();
    assert_eq!(
        normalize_list_config_resources_response(None, &response, MAX_NORMALIZED_BYTES),
        Err(ListConfigResourcesProtocolFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_list_config_resources_response(Some(0), &response, MAX_NORMALIZED_BYTES),
        Err(ListConfigResourcesProtocolFailure::UnsupportedApiVersion { actual: 0 })
    );

    let mut negative = response.clone();
    negative.throttle_time_ms = -1;
    assert_eq!(
        normalize_list_config_resources_response(Some(1), &negative, MAX_NORMALIZED_BYTES),
        Err(ListConfigResourcesProtocolFailure::NegativeThrottleTime { actual: -1 })
    );

    let mut invalid_type = response.clone();
    invalid_type.config_resources = vec![resource(0, "name")];
    assert_eq!(
        normalize_list_config_resources_response(Some(1), &invalid_type, MAX_NORMALIZED_BYTES),
        Err(ListConfigResourcesProtocolFailure::NonPositiveResourceType { actual: 0 })
    );

    let mut duplicate = response.clone();
    duplicate.config_resources = vec![resource(2, "same"), resource(2, "same")];
    assert_eq!(
        normalize_list_config_resources_response(Some(1), &duplicate, MAX_NORMALIZED_BYTES),
        Err(ListConfigResourcesProtocolFailure::DuplicateResource { resource_type: 2 })
    );

    let mut one = response;
    one.config_resources = vec![resource(2, "name")];
    assert!(matches!(
        normalize_list_config_resources_response(Some(1), &one, 1),
        Err(ListConfigResourcesProtocolFailure::RetainedBytes { .. })
    ));
}
