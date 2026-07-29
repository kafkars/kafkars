//! Version, scalar, hostile-shape, duplicate, and capacity rejection evidence.

use kafka_wire::{ListConfigResourcesResponse, list_config_resources_response::ConfigResource};

use super::{
    ListClientMetricsResourcesProtocolFailure, normalize_list_client_metrics_resources_response,
    validation::MAX_RESOURCE_NAME_BYTES,
};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn missing_nonzero_version_and_negative_throttle_are_rejected() {
    let response = ListConfigResourcesResponse::default();
    assert_eq!(
        normalize_list_client_metrics_resources_response(None, &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(1), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::UnsupportedApiVersion { actual: 1 })
    );
    let mut response = response;
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn broker_error_cannot_carry_a_success_payload() {
    let mut response = ListConfigResourcesResponse::default();
    response.error_code = 7;
    response.config_resources = vec![resource("orders", 16)];

    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::SuccessPayloadWithBrokerError)
    );
}

#[test]
fn v0_rejects_impossible_resource_types_empty_names_and_duplicates() {
    let mut response = ListConfigResourcesResponse::default();
    response.config_resources = vec![resource("orders", 15)];
    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::UnexpectedResourceType { actual: 15 })
    );

    response.config_resources = vec![resource("", 16)];
    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::EmptyResourceName)
    );

    response.config_resources = vec![resource("orders", 16), resource("orders", 16)];
    assert_eq!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT).err(),
        Some(ListClientMetricsResourcesProtocolFailure::DuplicateResourceName)
    );
}

#[test]
fn oversized_names_and_insufficient_retained_capacity_are_rejected() {
    let oversized = "x".repeat(MAX_RESOURCE_NAME_BYTES + 1);
    let mut response = ListConfigResourcesResponse::default();
    response.config_resources = vec![resource(&oversized, 16)];
    assert!(matches!(
        normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT),
        Err(ListClientMetricsResourcesProtocolFailure::ResourceNameTooLong { .. })
    ));

    response.config_resources = vec![resource("orders", 16)];
    assert!(matches!(
        normalize_list_client_metrics_resources_response(Some(0), &response, 0),
        Err(ListClientMetricsResourcesProtocolFailure::RetainedBytes { .. })
    ));
}

fn resource(name: &str, resource_type: i8) -> ConfigResource {
    let mut resource = ConfigResource::default();
    resource.resource_name = name.into();
    resource.resource_type = resource_type;
    resource
}
