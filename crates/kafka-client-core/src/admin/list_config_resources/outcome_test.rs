//! Successful resource, listing, and exact broker-rejection value scenarios.

use core::num::NonZeroI16;

use super::{
    ConfigResourceType, ListConfigResourcesBrokerError, ListConfigResourcesListing,
    ListedConfigResource,
};

#[test]
fn listing_and_resources_expose_stable_owned_parts() {
    let resource = ListedConfigResource::new(ConfigResourceType::TOPIC, "orders".to_owned());
    assert_eq!(resource.resource_type(), ConfigResourceType::TOPIC);
    assert_eq!(resource.resource_name(), "orders");
    assert_eq!(
        resource.clone().into_parts(),
        (ConfigResourceType::TOPIC, "orders".to_owned())
    );

    let listing = ListConfigResourcesListing::new(27, vec![resource.clone()]);
    assert_eq!(listing.throttle_time_ms(), 27);
    assert_eq!(listing.resources(), [resource.clone()]);
    assert_eq!(listing.into_parts(), (27, vec![resource]));
}

#[test]
fn broker_error_preserves_throttle_and_unknown_negative_code() {
    let error = ListConfigResourcesBrokerError::new(
        13,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero code")),
    );
    assert_eq!(error.throttle_time_ms(), 13);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.into_parts(), (13, -32_000));
}
