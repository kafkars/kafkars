//! Public Fetch-policy defaults and replacement evidence.

use std::time::Duration;

use super::ConsumerFetchConfig;

#[test]
fn defaults_are_finite_and_bounded() {
    let config = ConsumerFetchConfig::default();

    assert_eq!(config.max_wait(), Duration::from_millis(500));
    assert_eq!(config.min_bytes(), 1);
    assert_eq!(config.max_bytes(), 1024 * 1024);
    assert_eq!(config.partition_max_bytes(), 1024 * 1024);
    assert_eq!(config.attempt_timeout(), Duration::from_secs(30));
}

#[test]
fn every_fetch_dimension_is_replaced_independently() {
    let config = ConsumerFetchConfig::default()
        .with_max_wait(Duration::from_millis(750))
        .with_min_bytes(4_096)
        .with_max_bytes(8 * 1024 * 1024)
        .with_partition_max_bytes(2 * 1024 * 1024)
        .with_attempt_timeout(Duration::from_secs(17));

    assert_eq!(config.max_wait(), Duration::from_millis(750));
    assert_eq!(config.min_bytes(), 4_096);
    assert_eq!(config.max_bytes(), 8 * 1024 * 1024);
    assert_eq!(config.partition_max_bytes(), 2 * 1024 * 1024);
    assert_eq!(config.attempt_timeout(), Duration::from_secs(17));
}
