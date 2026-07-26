//! Host-side request for the concrete first assigned-consumer lifecycle.

use std::sync::Arc;

use crate::{
    clock::MonotonicClock,
    consumer::{
        AssignedConsumerClosePublisher, AssignedConsumerEventPublisher,
        AssignedConsumerOwnerBuildError, AssignedConsumerPort, AssignedConsumerRecvPublisher,
        AssignedConsumerShardOwner, build_first_assigned_consumer,
    },
    driver::ReactorWake,
};

pub(super) fn start_assigned_consumer(
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
    close_publisher: AssignedConsumerClosePublisher,
    recv_publisher: AssignedConsumerRecvPublisher,
    event_publisher: AssignedConsumerEventPublisher,
) -> Result<(AssignedConsumerShardOwner, AssignedConsumerPort), AssignedConsumerOwnerBuildError> {
    build_first_assigned_consumer(
        clock,
        wake,
        close_publisher,
        recv_publisher,
        event_publisher,
    )
}
