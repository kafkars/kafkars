//! Discovery-route and exact-broker compatibility scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::list_consumer_groups_submission::{
    list_consumer_groups_broker_options, list_consumer_groups_broker_route,
    list_consumer_groups_discovery_options, list_consumer_groups_discovery_route,
};

#[test]
fn discovery_is_controller_routed_and_brokers_are_exact_with_one_deadline() {
    let deadline = Instant::now() + Duration::from_secs(9);
    assert_eq!(list_consumer_groups_discovery_route(), Route::Controller);
    let discovery = list_consumer_groups_discovery_options(deadline);
    assert_eq!(discovery.deadline(), deadline);
    assert_eq!(discovery.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        discovery.maximum_version(),
        Some(kafka_driver::ApiVersion::new(2))
    );

    assert_eq!(list_consumer_groups_broker_route(7), Ok(Route::AnyBroker));
    assert!(list_consumer_groups_broker_route(-1).is_err());
    let broker = list_consumer_groups_broker_options(deadline, 0);
    assert_eq!(broker.deadline(), deadline);
    assert_eq!(broker.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        broker.minimum_version(),
        Some(kafka_driver::ApiVersion::new(0))
    );
    assert_eq!(
        broker.maximum_version(),
        Some(kafka_driver::ApiVersion::new(5))
    );
}
