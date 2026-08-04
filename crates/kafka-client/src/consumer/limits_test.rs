//! Public consumer-limit defaults and replacement evidence.

use super::ConsumerLimits;

#[test]
fn defaults_are_finite_and_independent() {
    let limits = ConsumerLimits::default();

    assert_eq!(limits.in_flight_fetches(), 8);
    assert_eq!(limits.buffered_batches(), 8);
    assert_eq!(limits.buffered_bytes(), 8 * 1024 * 1024);
    assert_eq!(limits.max_batch_bytes(), 1024 * 1024);
}

#[test]
fn every_limit_is_replaced_independently() {
    let limits = ConsumerLimits::default()
        .with_in_flight_fetches(3)
        .with_buffered_batches(5)
        .with_buffered_bytes(12 * 1024 * 1024)
        .with_max_batch_bytes(2 * 1024 * 1024);

    assert_eq!(limits.in_flight_fetches(), 3);
    assert_eq!(limits.buffered_batches(), 5);
    assert_eq!(limits.buffered_bytes(), 12 * 1024 * 1024);
    assert_eq!(limits.max_batch_bytes(), 2 * 1024 * 1024);
}
