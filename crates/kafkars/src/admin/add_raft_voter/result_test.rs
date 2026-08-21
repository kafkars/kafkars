//! Stable metadata-quorum voter-addition result tests.

use std::time::Duration;

use super::AddRaftVoterResult;

#[test]
fn result_preserves_kafka_throttle_observation() {
    let result = AddRaftVoterResult::new(Duration::from_millis(31));

    assert_eq!(result.throttle_time(), Duration::from_millis(31));
    assert_eq!(result.into_throttle_time(), Duration::from_millis(31));
}
