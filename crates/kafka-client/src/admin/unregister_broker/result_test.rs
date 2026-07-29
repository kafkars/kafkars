//! Stable Kafka broker-unregistration result tests.

use std::time::Duration;

use super::UnregisterBrokerResult;

#[test]
fn result_preserves_kafka_throttle_observation() {
    let result = UnregisterBrokerResult::new(Duration::from_millis(29));

    assert_eq!(result.throttle_time(), Duration::from_millis(29));
    assert_eq!(result.into_throttle_time(), Duration::from_millis(29));
}
