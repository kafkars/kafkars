//! Stable resource-type and resource-identity tests.

use super::{ConfigResource, ConfigResourceType};

#[test]
fn known_resource_type_constants_keep_exact_kafka_codes() {
    assert_eq!(ConfigResourceType::Topic.as_raw(), 2);
    assert_eq!(ConfigResourceType::Broker.as_raw(), 4);
    assert_eq!(ConfigResourceType::BrokerLogger.as_raw(), 8);
    assert_eq!(ConfigResourceType::ClientMetrics.as_raw(), 16);
    assert_eq!(ConfigResourceType::Group.as_raw(), 32);
}

#[test]
fn future_positive_type_codes_remain_lossless() {
    let future = ConfigResourceType::from_raw(64);
    let resource = ConfigResource::new(future, "future-resource".to_owned());

    assert_eq!(resource.resource_type(), future);
    assert_eq!(resource.name(), "future-resource");
    assert_eq!(
        resource.into_parts(),
        (future, "future-resource".to_owned())
    );
}

#[test]
fn invalid_request_codes_are_representable_until_submit_validation() {
    assert_eq!(ConfigResourceType::from_raw(0).as_raw(), 0);
    assert_eq!(ConfigResourceType::from_raw(-1).as_raw(), -1);
}
