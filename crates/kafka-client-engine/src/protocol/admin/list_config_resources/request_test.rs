//! Request validation and exact flexible-v1 construction evidence.

use kafka_wire::{KafkaMessage, KafkaRequest, ListConfigResourcesRequest};
use kafka_wire_core::ApiVersion;

use super::{
    ListConfigResourcesRequestFailure, list_config_resources_request,
    request::MAX_REQUEST_RESOURCE_TYPES,
};

#[test]
fn empty_selection_means_all_and_nonempty_selection_is_canonical() {
    assert!(
        list_config_resources_request(&[])
            .unwrap_or_else(|error| panic!("empty selection must be valid: {error:?}"))
            .resource_types
            .is_empty()
    );
    assert_eq!(
        list_config_resources_request(&[16, 2, 8])
            .unwrap_or_else(|error| panic!("selected resource types must be valid: {error:?}"))
            .resource_types,
        [2, 8, 16]
    );
    assert_eq!(
        <ListConfigResourcesRequest as KafkaRequest>::API_KEY.value(),
        74
    );
    assert!(ListConfigResourcesRequest::is_flexible(ApiVersion::new(1)));
}

#[test]
fn selection_rejects_nonpositive_duplicate_and_oversized_types() {
    assert_eq!(
        list_config_resources_request(&[0]),
        Err(ListConfigResourcesRequestFailure::NonPositiveResourceType { actual: 0 })
    );
    assert_eq!(
        list_config_resources_request(&[7, 7]),
        Err(ListConfigResourcesRequestFailure::DuplicateResourceType { actual: 7 })
    );
    let too_many = vec![1; MAX_REQUEST_RESOURCE_TYPES + 1];
    assert_eq!(
        list_config_resources_request(&too_many),
        Err(ListConfigResourcesRequestFailure::TooManyResourceTypes {
            actual: MAX_REQUEST_RESOURCE_TYPES + 1,
            max: MAX_REQUEST_RESOURCE_TYPES,
        })
    );
}
