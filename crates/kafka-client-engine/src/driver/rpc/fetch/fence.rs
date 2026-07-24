//! Directional control-effect fencing for exact retained partition Fetch calls.

use kafka_client_core::{AssignedConsumerEffect, FetchFence};

pub(super) fn supersedes(effect: AssignedConsumerEffect, candidate: FetchFence) -> bool {
    let position = candidate.position();
    match effect {
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition,
        } => position.assignment_epoch() == assignment_epoch && position.partition() == partition,
        AssignedConsumerEffect::Suspend { fence } => {
            position.assignment_epoch() == fence.assignment_epoch()
                && position.partition() == fence.partition()
                && position.position_epoch() < fence.position_epoch()
        }
        _ => false,
    }
}
