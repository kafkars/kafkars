//! Exact control-effect classification for retained position work.

use kafka_client_core::{AssignedConsumerEffect, PositionFence};

pub(super) fn supersedes(effect: AssignedConsumerEffect, candidate: PositionFence) -> bool {
    match effect {
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition,
        } => candidate.assignment_epoch() == assignment_epoch && candidate.partition() == partition,
        AssignedConsumerEffect::Suspend { fence } => {
            candidate.assignment_epoch() == fence.assignment_epoch()
                && candidate.partition() == fence.partition()
                && candidate.position_epoch() < fence.position_epoch()
        }
        _ => false,
    }
}
