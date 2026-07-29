//! Version and traffic policy tests for API27 transaction aborts.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, TrafficClass};

use super::abort_partition_transaction_submission::abort_partition_transaction_options;

#[test]
fn exact_v1_v2_interactive_window_preserves_deadline() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = abort_partition_transaction_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}
