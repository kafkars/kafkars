//! Engine configuration default and bounded-validation scenarios.

use std::time::Duration;

use crate::{EngineConfig, EngineProducerLimits};

#[test]
fn provisional_defaults_are_explicit_and_bounded() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()]);

    assert_eq!(config.delivery_timeout(), Duration::from_secs(30));
    assert!(config.validate().is_ok());
}

#[test]
fn zero_capacity_is_rejected_before_host_startup() {
    let limits = EngineProducerLimits::new(0, 1, 1, 1, Duration::from_millis(1));
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_producer_limits(limits);

    assert!(config.validate().is_err());
}

#[test]
fn compiler_preserves_accepted_capacity() {
    let limits = EngineProducerLimits::new(4_096, 11, 3, 2_048, Duration::from_millis(7));
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_producer_limits(limits);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("distinct capacities should compile: {error:?}"));

    assert_eq!(validated.host_limits.record_capacity, 11);
    assert_eq!(validated.host_limits.completion_capacity, 11);
    assert_eq!(validated.host_limits.batch_policy.max_records(), 3);
}
