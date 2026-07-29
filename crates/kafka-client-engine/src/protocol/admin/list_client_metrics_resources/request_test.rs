//! Exact empty flexible-v0 API-key 74 request evidence.

use kafka_wire::{KafkaMessage, KafkaRequest, ListConfigResourcesRequest};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use super::list_client_metrics_resources_request;

#[test]
fn request_selects_the_v0_default_client_metrics_resource_type() {
    let request = list_client_metrics_resources_request();

    assert!(request.resource_types.is_empty());
    assert!(request.unknown_tagged_fields.is_empty());
    assert_eq!(
        <ListConfigResourcesRequest as KafkaRequest>::API_KEY.value(),
        74
    );
    assert!(ListConfigResourcesRequest::is_flexible(ApiVersion::new(0)));
    assert!(request.encoded_len(ApiVersion::new(0)).is_ok());
}

#[test]
fn request_does_not_silently_expand_to_v1_resource_selection() {
    let mut request = list_client_metrics_resources_request();
    request.resource_types.push(16);

    assert!(request.encoded_len(ApiVersion::new(0)).is_err());
}
