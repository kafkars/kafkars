//! Exact error and canonical successful-name normalization evidence.

use kafka_wire::{ListConfigResourcesResponse, list_config_resources_response::ConfigResource};

use super::normalize_list_client_metrics_resources_response;

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn successful_names_are_copied_and_ordered_by_utf8_bytes() {
    let mut response = ListConfigResourcesResponse::default();
    response.throttle_time_ms = 23;
    response.config_resources = vec![resource("zeta"), resource("alpha")];

    let normalized = normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let (throttle, code, names, retained) = normalized.into_parts();

    assert_eq!(throttle, 23);
    assert_eq!(code, 0);
    assert_eq!(names, ["alpha", "zeta"]);
    assert!(retained > 0);
}

#[test]
fn unknown_signed_top_level_error_is_lossless_and_has_no_success_payload() {
    let mut response = ListConfigResourcesResponse::default();
    response.error_code = -32_000;

    let normalized = normalize_list_client_metrics_resources_response(Some(0), &response, LIMIT)
        .unwrap_or_else(|error| panic!("valid broker error: {error:?}"));
    let (throttle, code, names, retained) = normalized.into_parts();

    assert_eq!((throttle, code), (0, -32_000));
    assert!(names.is_empty());
    assert!(retained > 0);
}

fn resource(name: &str) -> ConfigResource {
    let mut resource = ConfigResource::default();
    resource.resource_name = name.into();
    resource
}
