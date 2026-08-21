//! Exact consumer-configuration bridge mapping evidence.

use std::time::Duration;

use kafka_client_engine::ConsumerReadIsolation as EngineReadIsolation;

use super::consumer_configuration::{
    engine_classic_group_config, engine_consumer_fetch, engine_consumer_limits,
    engine_group_consumer_operations, engine_read_isolation,
};
use crate::{
    ClassicGroupConfig, ConsumerFetchConfig, ConsumerLimits, GroupConsumerOperationConfig,
    ReadIsolation,
};

#[test]
fn consumer_fetch_and_capacity_values_cross_the_bridge_exactly() {
    let fetch = engine_consumer_fetch(ConsumerFetchConfig::new(
        Duration::from_millis(125),
        4_096,
        2 * 1024 * 1024,
        512 * 1024,
        Duration::from_secs(7),
    ));
    assert_eq!(fetch.max_wait(), Duration::from_millis(125));
    assert_eq!(fetch.min_bytes(), 4_096);
    assert_eq!(fetch.max_bytes(), 2 * 1024 * 1024);
    assert_eq!(fetch.partition_max_bytes(), 512 * 1024);
    assert_eq!(fetch.attempt_timeout(), Duration::from_secs(7));

    let limits = engine_consumer_limits(ConsumerLimits::new(3, 5, 4 * 1024 * 1024, 512 * 1024));
    assert_eq!(limits.in_flight_fetches(), 3);
    assert_eq!(limits.buffered_batches(), 5);
    assert_eq!(limits.buffered_bytes(), 4 * 1024 * 1024);
    assert_eq!(limits.max_batch_bytes(), 512 * 1024);
}

#[test]
fn classic_group_timing_crosses_the_bridge_exactly() {
    let config = engine_classic_group_config(ClassicGroupConfig::new(
        Duration::from_secs(11),
        Duration::from_secs(31),
        Duration::from_secs(4),
        Duration::from_secs(12),
        Duration::from_secs(2),
        Duration::from_secs(32),
    ));

    assert_eq!(config.session_timeout(), Duration::from_secs(11));
    assert_eq!(config.rebalance_timeout(), Duration::from_secs(31));
    assert_eq!(config.heartbeat_interval(), Duration::from_secs(4));
    assert_eq!(config.heartbeat_attempt_timeout(), Duration::from_secs(12));
    assert_eq!(config.rejoin_backoff(), Duration::from_secs(2));
    assert_eq!(config.rejoin_attempt_timeout(), Duration::from_secs(32));
}

#[test]
fn group_operation_durations_cross_the_bridge_exactly() {
    let config = engine_group_consumer_operations(GroupConsumerOperationConfig::new(
        Duration::from_secs(11),
        Duration::from_secs(17),
    ));

    assert_eq!(config.seek_timeout(), Duration::from_secs(11));
    assert_eq!(config.close_timeout(), Duration::from_secs(17));
}

#[test]
fn read_isolation_maps_exhaustively() {
    for (public, engine) in [
        (
            ReadIsolation::ReadUncommitted,
            EngineReadIsolation::ReadUncommitted,
        ),
        (
            ReadIsolation::ReadCommitted,
            EngineReadIsolation::ReadCommitted,
        ),
    ] {
        assert_eq!(engine_read_isolation(public), engine);
    }
}
