//! Engine configuration default and bounded-validation scenarios.

use std::time::Duration;

use kafka_client_core::{ProducerRetryPolicy, ProducerRetryPolicyError};

use crate::{EngineConfig, EngineProducerLimits, config::EngineConfigError};

#[test]
fn provisional_defaults_are_explicit_and_bounded() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()]);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("default config failed: {error:?}"));

    assert_eq!(config.delivery_timeout(), Duration::from_secs(30));
    assert_eq!(config.admin_timeout(), Duration::from_secs(30));
    assert_eq!(
        validated.host_limits.retry_policy,
        ProducerRetryPolicy::try_fixed(3, 100_000_000)
            .unwrap_or_else(|error| panic!("default retry failed: {error}"))
    );
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
