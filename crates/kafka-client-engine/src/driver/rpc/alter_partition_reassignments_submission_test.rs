//! Controller routing and exact reassignment version bounds.

use std::time::Instant;

use kafka_driver::{ApiVersion, TrafficClass};

use super::alter_partition_reassignments_submission::alter_partition_reassignments_options;

#[test]
fn reassignment_uses_original_deadline_lane_and_policy_specific_floor() {
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    let default_options = alter_partition_reassignments_options(deadline, true);
    assert_eq!(default_options.deadline(), deadline);
    assert_eq!(default_options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(default_options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(default_options.maximum_version(), Some(ApiVersion::new(1)));

    let disallow_options = alter_partition_reassignments_options(deadline, false);
    assert_eq!(disallow_options.deadline(), deadline);
    assert_eq!(disallow_options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(disallow_options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(disallow_options.maximum_version(), Some(ApiVersion::new(1)));
}
