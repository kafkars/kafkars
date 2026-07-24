//! Route and option scenarios for tracked topic `IncrementalAlterConfigs` submission.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::incremental_alter_configs_submission::{
    incremental_alter_configs_options, incremental_alter_configs_route,
};

#[test]
fn submission_uses_any_broker_interactive_lane_original_deadline_and_v1_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = incremental_alter_configs_options(deadline);
    assert_eq!(incremental_alter_configs_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(1))
    );
}
