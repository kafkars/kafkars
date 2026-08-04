//! Engine configuration default and bounded-validation scenarios.

use std::time::Duration;

use kafka_client_core::{ProducerRetryPolicy, ProducerRetryPolicyError};

use crate::{
    ConsumerReadIsolation, EngineConfig, EngineConsumerFetchConfig, EngineConsumerLimits,
    EngineProducerLimits, ProducerCompression, config::EngineConfigError,
};

#[test]
fn provisional_defaults_are_explicit_and_bounded() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()]);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("default config failed: {error:?}"));

    assert_eq!(config.delivery_timeout(), Duration::from_secs(30));
    assert_eq!(config.admin_timeout(), Duration::from_secs(30));
    assert_eq!(config.producer_compression(), ProducerCompression::None);
    assert_eq!(
        config.assigned_consumer_read_isolation(),
        ConsumerReadIsolation::ReadUncommitted
    );
    assert_eq!(
        config.assigned_consumer_fetch(),
        EngineConsumerFetchConfig::default()
    );
    assert_eq!(
        config.assigned_consumer_limits(),
        EngineConsumerLimits::default()
    );
    assert_eq!(
        validated.host_limits.compression,
        kafka_client_core::CompressionPolicy::None
    );
    assert_eq!(
        validated.host_limits.retry_policy,
        ProducerRetryPolicy::try_fixed(3, 100_000_000)
            .unwrap_or_else(|error| panic!("default retry failed: {error}"))
    );
}

#[test]
fn assigned_consumer_limits_compile_and_cover_partition_fetch_bytes() {
    let limits = EngineConsumerLimits::new(3, 5, 4 * 1024 * 1024, 512 * 1024);
    let default_fetch = EngineConsumerFetchConfig::default();
    let fetch = EngineConsumerFetchConfig::new(
        default_fetch.max_wait(),
        default_fetch.min_bytes(),
        default_fetch.max_bytes(),
        512 * 1024,
        default_fetch.attempt_timeout(),
    );
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_assigned_consumer_fetch(fetch)
        .with_assigned_consumer_limits(limits);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("valid assigned consumer limits failed: {error:?}"));

    assert_eq!(config.assigned_consumer_limits(), limits);
    assert_eq!(validated.assigned_consumer_limits.in_flight_fetches(), 3);
    assert_eq!(validated.assigned_consumer_limits.buffered_batches(), 5);
    assert_eq!(
        validated.assigned_consumer_limits.buffered_bytes(),
        4 * 1024 * 1024
    );
    assert_eq!(
        validated.assigned_consumer_limits.max_batch_bytes(),
        512 * 1024
    );

    let invalid = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_assigned_consumer_limits(EngineConsumerLimits::new(1, 1, 1024 * 1024, 512 * 1024));
    assert!(invalid.validate().is_err());
}

#[test]
fn assigned_consumer_fetch_is_compiled_before_host_start() {
    let fetch = EngineConsumerFetchConfig::new(
        Duration::from_millis(125),
        2_048,
        2 * 1024 * 1024,
        512 * 1024,
        Duration::from_secs(7),
    );
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_assigned_consumer_fetch(fetch);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("valid assigned Fetch config failed: {error:?}"));

    assert_eq!(config.assigned_consumer_fetch(), fetch);
    assert_eq!(validated.assigned_consumer_fetch.max_wait_ms(), 125);
    assert_eq!(validated.assigned_consumer_fetch.min_bytes(), 2_048);
    assert_eq!(
        validated.assigned_consumer_fetch.max_bytes(),
        2 * 1024 * 1024
    );
    assert_eq!(
        validated.assigned_consumer_fetch.partition_max_bytes(),
        512 * 1024
    );
    assert_eq!(
        validated.assigned_consumer_fetch.attempt_timeout(),
        Duration::from_secs(7)
    );
}

#[test]
fn assigned_consumer_read_isolation_is_closed_and_immutable_before_start() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_assigned_consumer_read_isolation(ConsumerReadIsolation::ReadCommitted);

    assert_eq!(
        config.assigned_consumer_read_isolation(),
        ConsumerReadIsolation::ReadCommitted
    );
    assert!(config.validate().is_ok());
}

#[test]
fn every_compression_choice_translates_exhaustively_into_core_policy() {
    let pairs = [
        (
            ProducerCompression::None,
            kafka_client_core::CompressionPolicy::None,
        ),
        (
            ProducerCompression::Gzip,
            kafka_client_core::CompressionPolicy::Gzip,
        ),
        (
            ProducerCompression::Snappy,
            kafka_client_core::CompressionPolicy::Snappy,
        ),
        (
            ProducerCompression::Lz4,
            kafka_client_core::CompressionPolicy::Lz4,
        ),
        (
            ProducerCompression::Zstd,
            kafka_client_core::CompressionPolicy::Zstd,
        ),
    ];
    for (requested, expected) in pairs {
        let validated = EngineConfig::new(vec!["broker.test:9092".to_owned()])
            .with_producer_compression(requested)
            .validate()
            .unwrap_or_else(|error| panic!("compression config failed: {error:?}"));
        assert_eq!(validated.host_limits.compression, expected);
    }
}

#[test]
fn zero_retry_count_canonicalizes_disabled_even_with_zero_backoff() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_producer_retry(0, Duration::ZERO);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("disabled retry failed: {error:?}"));

    assert_eq!(
        validated.host_limits.retry_policy,
        ProducerRetryPolicy::none()
    );
}

#[test]
fn disabled_retry_ignores_unrepresentable_backoff() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_producer_retry(0, Duration::MAX);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("disabled retry failed: {error:?}"));

    assert_eq!(
        validated.host_limits.retry_policy,
        ProducerRetryPolicy::none()
    );
}

#[test]
fn enabled_retry_rejects_zero_backoff_before_host_start() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_producer_retry(1, Duration::ZERO);

    assert_eq!(
        config.validate().err(),
        Some(EngineConfigError::RetryPolicy(
            ProducerRetryPolicyError::ZeroBackoff
        ))
    );
}

#[test]
fn admin_timeout_is_engine_owned_and_validated() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()])
        .with_admin_timeout(Duration::from_secs(7));
    assert_eq!(config.admin_timeout(), Duration::from_secs(7));
    assert!(config.validate().is_ok());

    let zero =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_admin_timeout(Duration::ZERO);
    assert!(zero.validate().is_err());
}

#[test]
fn zero_capacity_is_rejected_before_host_startup() {
    let limits = EngineProducerLimits::new(0, 1, 1, 1, 1, 1, Duration::from_millis(1));
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_producer_limits(limits);

    assert!(config.validate().is_err());
}

#[test]
fn compiler_preserves_accepted_capacity() {
    let limits = EngineProducerLimits::new(4_096, 11, 5, 2_000, 3, 2_048, Duration::from_millis(7));
    let config =
        EngineConfig::new(vec!["broker.test:9092".to_owned()]).with_producer_limits(limits);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("distinct capacities should compile: {error:?}"));

    assert_eq!(validated.host_limits.record_capacity, 11);
    assert_eq!(validated.host_limits.completion_capacity, 11);
    assert_eq!(validated.host_limits.waiting_record_capacity, 5);
    assert_eq!(validated.host_limits.waiting_byte_capacity, 2_000);
    assert_eq!(validated.host_limits.batch_policy.max_records(), 3);
}
