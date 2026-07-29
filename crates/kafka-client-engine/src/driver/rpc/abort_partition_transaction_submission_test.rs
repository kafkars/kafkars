//! Version and traffic policy tests for API27 transaction aborts.

use std::time::{Duration, Instant};

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_driver::{ApiVersion, TrafficClass};

use super::abort_partition_transaction_submission::abort_partition_transaction_options;

#[test]
fn default_transaction_version_keeps_exact_v1_v2_interactive_window() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let plan = plan();
    let options = abort_partition_transaction_options(&plan, deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn transaction_version_two_raises_the_floor_to_v2() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let plan = plan()
        .with_transaction_version(2)
        .expect("valid transaction version");
    let options = abort_partition_transaction_options(&plan, deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

fn plan() -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11).expect("valid abort plan")
}
