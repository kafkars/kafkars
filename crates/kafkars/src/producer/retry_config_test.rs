//! Public producer retry defaults and independent replacement evidence.

use std::time::Duration;

use super::ProducerRetryConfig;

#[test]
fn default_and_disabled_policies_are_explicit() {
    let default = ProducerRetryConfig::default();
    assert_eq!(default.max_retries(), 10);
    assert_eq!(default.backoff(), Duration::from_millis(100));

    let disabled = ProducerRetryConfig::disabled();
    assert_eq!(disabled.max_retries(), 0);
    assert_eq!(disabled.backoff(), Duration::ZERO);
}

#[test]
fn retry_bound_and_backoff_are_replaced_independently() {
    let retry = ProducerRetryConfig::default()
        .with_max_retries(7)
        .with_backoff(Duration::from_millis(250));

    assert_eq!(retry.max_retries(), 7);
    assert_eq!(retry.backoff(), Duration::from_millis(250));
}
