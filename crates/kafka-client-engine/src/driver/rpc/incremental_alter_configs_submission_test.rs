//! Route and option scenarios for tracked resource-generic `IncrementalAlterConfigs`.

use std::time::{Duration, Instant};

use kafka_client_core::IncrementalAlterConfigsRoute;
use kafka_driver::{BrokerId, Route, TrafficClass};

use super::incremental_alter_configs_submission::{
    incremental_alter_configs_options, incremental_alter_configs_route,
};

#[test]
fn submission_preserves_route_interactive_lane_original_deadline_and_v1_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = incremental_alter_configs_options(deadline);
    assert_eq!(
        incremental_alter_configs_route(IncrementalAlterConfigsRoute::AnyBroker)
            .unwrap_or_else(|error| panic!("any-broker route: {error}")),
        Route::AnyBroker
    );
    assert_eq!(
        incremental_alter_configs_route(IncrementalAlterConfigsRoute::ExactBroker(7))
            .unwrap_or_else(|error| panic!("exact broker route: {error}")),
        Route::Broker {
            broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker: {error}")),
        }
    );
    assert!(
        incremental_alter_configs_route(IncrementalAlterConfigsRoute::ExactBroker(-1)).is_err()
    );
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(1))
    );
}
