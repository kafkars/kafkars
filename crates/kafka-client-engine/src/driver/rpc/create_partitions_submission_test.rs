//! Controller route options for tracked `CreatePartitions` submission.

use std::time::{Duration, Instant};

use kafka_driver::TrafficClass;

use super::create_partitions_submission::create_partitions_options;

#[test]
fn create_partitions_uses_interactive_original_deadline_and_v3_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = create_partitions_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(3))
    );
}
