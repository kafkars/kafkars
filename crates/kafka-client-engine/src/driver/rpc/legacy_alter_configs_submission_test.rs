//! Route and version policy for legacy resource configuration replacement.

use std::time::{Duration, Instant};

use kafka_client_core::LegacyAlterConfigsRoute;
use kafka_driver::{ApiVersion, BrokerId, Route, TrafficClass};

use super::legacy_alter_configs_submission::{
    legacy_alter_configs_options, legacy_alter_configs_route,
};

#[test]
fn submission_preserves_route_interactive_deadline_and_full_stable_range() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = legacy_alter_configs_options(deadline);
    assert_eq!(
        legacy_alter_configs_route(LegacyAlterConfigsRoute::AnyBroker)
            .unwrap_or_else(|error| panic!("any-broker route: {error}")),
        Route::AnyBroker
    );
    assert_eq!(
        legacy_alter_configs_route(LegacyAlterConfigsRoute::ExactBroker(7))
            .unwrap_or_else(|error| panic!("exact broker route: {error}")),
        Route::Broker {
            broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("broker ID: {error}")),
        }
    );
    assert!(legacy_alter_configs_route(LegacyAlterConfigsRoute::ExactBroker(-1)).is_err());
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}
