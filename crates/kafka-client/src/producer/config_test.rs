//! Complete producer configuration defaults and replacement evidence.

use std::time::Duration;

use super::{Compression, ProducerConfig, ProducerLimits, ProducerRetryConfig};

#[test]
fn defaults_name_every_configurable_producer_policy() {
    let config = ProducerConfig::default();

    assert_eq!(config.delivery_timeout(), Duration::from_secs(30));
    assert_eq!(config.compression(), Compression::None);
    assert_eq!(config.retry(), ProducerRetryConfig::default());
    assert_eq!(config.limits(), ProducerLimits::default());
}

#[test]
fn each_policy_is_replaced_without_weakening_fixed_durability() {
    let retry = ProducerRetryConfig::new(5, Duration::from_millis(250));
    let limits = ProducerLimits::default().with_linger(Duration::from_millis(20));
    let config = ProducerConfig::default()
        .with_delivery_timeout(Duration::from_secs(45))
        .with_compression(Compression::Zstd)
        .with_retry(retry)
        .with_limits(limits);

    assert_eq!(config.delivery_timeout(), Duration::from_secs(45));
    assert_eq!(config.compression(), Compression::Zstd);
    assert_eq!(config.retry(), retry);
    assert_eq!(config.limits(), limits);
}
