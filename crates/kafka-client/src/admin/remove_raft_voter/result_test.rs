//! Stable metadata-quorum voter-removal result tests.

use std::time::Duration;

use super::RemoveRaftVoterResult;

#[test]
fn result_preserves_kafka_throttle_observation() {
    let result = RemoveRaftVoterResult::new(Duration::from_millis(37));

    assert_eq!(result.throttle_time(), Duration::from_millis(37));
    assert_eq!(result.into_throttle_time(), Duration::from_millis(37));
}
