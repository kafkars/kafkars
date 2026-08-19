//! Host-side request for the concrete first assigned-consumer lifecycle.

use std::sync::Arc;

use kafka_client_core::ReadIsolation;

use crate::{
    clock::MonotonicClock,
    config::{ValidatedConsumerFetchConfig, ValidatedConsumerLimits},
    consumer::{
        AssignedConsumerClosePublisher, AssignedConsumerEventPublisher,
        AssignedConsumerOwnerBuildError, AssignedConsumerPort, AssignedConsumerRecvPublisher,
        AssignedConsumerShardOwner, build_first_assigned_consumer,
    },
    driver::ReactorWake,
};

#[allow(
    clippy::too_many_arguments,
    reason = "the startup handoff transfers each bounded completion and wake capability explicitly"
)]
pub(super) fn start_assigned_consumer(
    read_isolation: ReadIsolation,
    fetch: ValidatedConsumerFetchConfig,
    limits: ValidatedConsumerLimits,
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
    close_publisher: AssignedConsumerClosePublisher,
    recv_publisher: AssignedConsumerRecvPublisher,
    event_publisher: AssignedConsumerEventPublisher,
) -> Result<(AssignedConsumerShardOwner, AssignedConsumerPort), AssignedConsumerOwnerBuildError> {
    build_first_assigned_consumer(
        read_isolation,
        fetch,
        limits,
        clock,
        wake,
        close_publisher,
        recv_publisher,
        event_publisher,
    )
}
