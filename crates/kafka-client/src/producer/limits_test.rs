//! Public producer limit defaults and const builder round trips.

use std::time::Duration;

use super::ProducerLimits;

#[test]
fn defaults_expose_all_active_waiting_and_batch_limits() {
    let limits = ProducerLimits::default();

    assert_eq!(limits.retained_bytes(), 32 * 1024 * 1024);
    assert_eq!(limits.in_flight_records(), 1_024);
    assert_eq!(limits.waiting_records(), 1_024);
    assert_eq!(limits.waiting_bytes(), 32 * 1024 * 1024);
    assert_eq!(limits.batch_records(), 256);
    assert_eq!(limits.batch_bytes(), 1024 * 1024);
    assert_eq!(limits.linger(), Duration::from_millis(5));
}

#[test]
fn named_setters_round_trip_without_seven_positional_arguments() {
    const LIMITS: ProducerLimits = ProducerLimits::new(1, 2, 3, 4, 5, 6, Duration::from_millis(7))
        .with_retained_bytes(11)
        .with_in_flight_records(12)
        .with_waiting_records(13)
        .with_waiting_bytes(14)
        .with_batch_records(15)
        .with_batch_bytes(16)
        .with_linger(Duration::from_millis(17));

    assert_eq!(LIMITS.retained_bytes(), 11);
    assert_eq!(LIMITS.in_flight_records(), 12);
    assert_eq!(LIMITS.waiting_records(), 13);
    assert_eq!(LIMITS.waiting_bytes(), 14);
    assert_eq!(LIMITS.batch_records(), 15);
    assert_eq!(LIMITS.batch_bytes(), 16);
    assert_eq!(LIMITS.linger(), Duration::from_millis(17));
}
