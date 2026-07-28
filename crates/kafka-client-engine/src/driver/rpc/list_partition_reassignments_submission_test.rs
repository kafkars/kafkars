//! Controller route and exact v0 options for reassignment listing.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, TrafficClass};

use super::list_partition_reassignments_submission::list_partition_reassignments_options;

#[test]
fn reassignment_listing_uses_interactive_original_deadline_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = list_partition_reassignments_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
