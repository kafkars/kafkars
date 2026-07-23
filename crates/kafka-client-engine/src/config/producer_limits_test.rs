//! Producer-limit default and explicit-capacity scenarios.

use std::time::Duration;

use super::EngineProducerLimits;

#[test]
fn defaults_name_each_bounded_producer_resource() {
    let limits = EngineProducerLimits::default();

    assert_eq!(limits.retained_bytes(), 32 * 1024 * 1024);
    assert_eq!(limits.in_flight_records(), 1_024);
    assert_eq!(limits.batch_records(), 256);
    assert_eq!(limits.batch_bytes(), 1024 * 1024);
    assert_eq!(limits.linger(), Duration::from_millis(5));
}

#[test]
fn explicit_limits_preserve_accepted_record_capacity() {
    let limits = EngineProducerLimits::new(4_096, 11, 3, 2_048, Duration::from_millis(7));

    assert_eq!(limits.in_flight_records(), 11);
    assert_eq!(limits.batch_records(), 3);
}
