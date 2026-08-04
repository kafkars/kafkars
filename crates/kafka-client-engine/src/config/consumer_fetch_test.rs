//! Fetch-configuration validation evidence.

use std::time::Duration;

use super::{ConsumerFetchConfigError, EngineConsumerFetchConfig};

#[test]
fn default_fetch_policy_compiles_exactly() {
    let validated = EngineConsumerFetchConfig::default()
        .validate()
        .unwrap_or_else(|error| panic!("default Fetch policy rejected: {error:?}"));

    assert_eq!(validated.max_wait_ms(), 500);
    assert_eq!(validated.min_bytes(), 1);
    assert_eq!(validated.max_bytes(), 1024 * 1024);
    assert_eq!(validated.partition_max_bytes(), 1024 * 1024);
    assert_eq!(validated.attempt_timeout(), Duration::from_secs(30));
}

#[test]
fn invalid_fetch_dimensions_are_closed() {
    let default = EngineConsumerFetchConfig::default();
    let cases = [
        (
            EngineConsumerFetchConfig::new(
                Duration::ZERO,
                default.min_bytes(),
                default.max_bytes(),
                default.partition_max_bytes(),
                default.attempt_timeout(),
            ),
            ConsumerFetchConfigError::MaxWait,
        ),
        (
            EngineConsumerFetchConfig::new(
                Duration::from_nanos(1),
                default.min_bytes(),
                default.max_bytes(),
                default.partition_max_bytes(),
                default.attempt_timeout(),
            ),
            ConsumerFetchConfigError::MaxWait,
        ),
        (
            EngineConsumerFetchConfig::new(
                default.max_wait(),
                2,
                1,
                default.partition_max_bytes(),
                default.attempt_timeout(),
            ),
            ConsumerFetchConfigError::MinBytesExceedMaxBytes,
        ),
        (
            EngineConsumerFetchConfig::new(
                default.max_wait(),
                default.min_bytes(),
                0,
                default.partition_max_bytes(),
                default.attempt_timeout(),
            ),
            ConsumerFetchConfigError::MaxBytes,
        ),
        (
            EngineConsumerFetchConfig::new(
                default.max_wait(),
                default.min_bytes(),
                default.max_bytes(),
                0,
                default.attempt_timeout(),
            ),
            ConsumerFetchConfigError::PartitionMaxBytes,
        ),
        (
            EngineConsumerFetchConfig::new(
                default.max_wait(),
                default.min_bytes(),
                default.max_bytes(),
                default.partition_max_bytes(),
                Duration::ZERO,
            ),
            ConsumerFetchConfigError::AttemptTimeout,
        ),
    ];

    for (config, expected) in cases {
        assert_eq!(config.validate(), Err(expected));
    }
}
