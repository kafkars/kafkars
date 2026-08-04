//! First direct-consumer slice compiles one bounded immutable-isolation lifecycle.

use std::sync::Arc;

use kafka_client_core::ReadIsolation;

use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::super::{
    assigned_owner_model::{
        AssignedConsumerOwnerBuildError, AssignedConsumerOwnerLimits, AssignedConsumerOwnerSettings,
    },
    assigned_topics::AssignedTopicLimits,
};
use super::{
    completion::{
        AssignedConsumerClosePublisher, AssignedConsumerEventPublisher,
        AssignedConsumerRecvPublisher,
    },
    shard::{AssignedConsumerPort, AssignedConsumerShardOwner},
    wake::AssignedConsumerShardWake,
};

const PARTITIONS: usize = 64;
pub(crate) fn build_first_assigned_consumer<W>(
    read_isolation: ReadIsolation,
    fetch: ValidatedConsumerFetchConfig,
    limits: ValidatedConsumerLimits,
    clock: Arc<MonotonicClock>,
    wake: Arc<W>,
    close_publisher: AssignedConsumerClosePublisher,
    recv_publisher: AssignedConsumerRecvPublisher,
    event_publisher: AssignedConsumerEventPublisher,
) -> Result<(AssignedConsumerShardOwner, AssignedConsumerPort), AssignedConsumerOwnerBuildError>
where
    W: AssignedConsumerShardWake,
{
    let settings = AssignedConsumerOwnerSettings::new(
        read_isolation,
        FetchRequestSettings::new(
            fetch.max_wait_ms(),
            fetch.min_bytes(),
            fetch.max_bytes(),
            fetch.partition_max_bytes(),
            0,
        ),
        FetchDecodeLimits::default(),
        fetch.attempt_timeout(),
        8,
    );
    let limits = AssignedConsumerOwnerLimits::new(
        PARTITIONS,
        limits.in_flight_fetches(),
        limits.buffered_batches(),
        limits.buffered_bytes(),
        limits.max_batch_bytes(),
        AssignedTopicLimits::new(PARTITIONS, PARTITIONS, 249, 16 * 1024),
    )?;
    AssignedConsumerShardOwner::new(
        clock,
        settings,
        limits,
        wake,
        close_publisher,
        recv_publisher,
        event_publisher,
    )
}
