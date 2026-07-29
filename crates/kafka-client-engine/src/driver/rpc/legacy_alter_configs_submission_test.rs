//! Route and version policy for legacy resource configuration replacement.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::legacy_alter_configs_submission::{
    legacy_alter_configs_options, legacy_alter_configs_route,
};

#[test]
fn submission_uses_any_broker_interactive_original_deadline_and_full_stable_range() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = legacy_alter_configs_options(deadline);
    assert_eq!(legacy_alter_configs_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}
