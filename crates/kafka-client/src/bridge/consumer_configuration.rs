//! Lossless facade-to-engine translation for immutable consumer configuration.

use kafka_client_engine::{
    ConsumerReadIsolation as EngineReadIsolation, EngineClassicGroupConfig,
    EngineConsumerFetchConfig, EngineConsumerLimits, EngineGroupConsumerOperationConfig,
};

use crate::{
    ClassicGroupConfig, ConsumerFetchConfig, ConsumerLimits, GroupConsumerOperationConfig,
    ReadIsolation,
};

pub(super) const fn engine_classic_group_config(
    config: ClassicGroupConfig,
) -> EngineClassicGroupConfig {
    let (
        session_timeout,
        rebalance_timeout,
        heartbeat_interval,
        heartbeat_attempt_timeout,
        rejoin_backoff,
        rejoin_attempt_timeout,
    ) = config.into_parts();
    EngineClassicGroupConfig::new(
        session_timeout,
        rebalance_timeout,
        heartbeat_interval,
        heartbeat_attempt_timeout,
        rejoin_backoff,
        rejoin_attempt_timeout,
    )
}

pub(super) const fn engine_group_consumer_operations(
    config: GroupConsumerOperationConfig,
) -> EngineGroupConsumerOperationConfig {
    let (seek_timeout, close_timeout) = config.into_parts();
    EngineGroupConsumerOperationConfig::new(seek_timeout, close_timeout)
}

pub(super) const fn engine_consumer_fetch(fetch: ConsumerFetchConfig) -> EngineConsumerFetchConfig {
    let (max_wait, min_bytes, max_bytes, partition_max_bytes, attempt_timeout) = fetch.into_parts();
    EngineConsumerFetchConfig::new(
        max_wait,
        min_bytes,
        max_bytes,
        partition_max_bytes,
        attempt_timeout,
    )
}

pub(super) const fn engine_consumer_limits(limits: ConsumerLimits) -> EngineConsumerLimits {
    let (in_flight_fetches, buffered_batches, buffered_bytes, max_batch_bytes) =
        limits.into_parts();
    EngineConsumerLimits::new(
        in_flight_fetches,
        buffered_batches,
        buffered_bytes,
        max_batch_bytes,
    )
}

pub(super) const fn engine_read_isolation(read_isolation: ReadIsolation) -> EngineReadIsolation {
    match read_isolation {
        ReadIsolation::ReadUncommitted => EngineReadIsolation::ReadUncommitted,
        ReadIsolation::ReadCommitted => EngineReadIsolation::ReadCommitted,
    }
}
