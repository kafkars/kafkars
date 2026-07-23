//! Engine configuration default and bounded-validation scenarios.

use std::time::Duration;

use crate::{EngineConfig, EngineProducerLimits};

#[test]
fn provisional_defaults_are_explicit_and_bounded() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()]);
    let limits = config.producer_limits();

    assert_eq!(config.delivery_timeout(), Duration::from_secs(30));
    assert_eq!(limits.retained_bytes(), 32 * 1024 * 1024);
    assert_eq!(limits.in_flight_records(), 1_024);
    assert_eq!(limits.batch_records(), 256);
    assert_eq!(limits.batch_bytes(), 1024 * 1024);
    assert_eq!(limits.linger(), Duration::from_millis(5));
    assert!(config.validate().is_ok());
}

#[test]
fn zero_capacity_is_rejected_before_host_startup() {
    let limits = EngineProducerLimits::new(0, 1, 1, 1, Duration::from_millis(1));
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_producer_limits(limits);

    assert!(config.validate().is_err());
}
