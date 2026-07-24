//! Host-side request for the concrete first assigned-consumer lifecycle.

use std::sync::Arc;

use crate::{
    clock::MonotonicClock,
    consumer::{
        AssignedConsumerOwnerBuildError, AssignedConsumerPort, AssignedConsumerShardOwner,
        build_first_assigned_consumer,
    },
    driver::ReactorWake,
};

pub(super) fn start_assigned_consumer(
    clock: Arc<MonotonicClock>,
    wake: Arc<ReactorWake>,
) -> Result<(AssignedConsumerShardOwner, AssignedConsumerPort), AssignedConsumerOwnerBuildError> {
    build_first_assigned_consumer(clock, wake)
}
