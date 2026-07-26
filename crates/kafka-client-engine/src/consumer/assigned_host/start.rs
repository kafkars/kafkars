//! First direct-consumer slice compiles one bounded read-uncommitted lifecycle.

use std::{sync::Arc, time::Duration};

use crate::{
    clock::MonotonicClock,
    protocol::{
        consumer::ListOffsetsIsolation,
        fetch::{FetchDecodeLimits, FetchRequestSettings},
    },
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
const CALLS: usize = 8;
const DELIVERIES: usize = 8;
const DELIVERY_BYTES: usize = 8 * 1024 * 1024;
const FETCH_REQUEST_BYTES: u32 = 1024 * 1024;
const FETCH_OUTPUT_BYTES: usize = 1024 * 1024;

pub(crate) fn build_first_assigned_consumer<W>(
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
        ListOffsetsIsolation::ReadUncommitted,
        FetchRequestSettings::new(500, 1, FETCH_REQUEST_BYTES, FETCH_REQUEST_BYTES, 0),
        FetchDecodeLimits::default(),
        Duration::from_secs(30),
        8,
    );
    let limits = AssignedConsumerOwnerLimits::new(
        PARTITIONS,
        CALLS,
        DELIVERIES,
        DELIVERY_BYTES,
        FETCH_OUTPUT_BYTES,
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
