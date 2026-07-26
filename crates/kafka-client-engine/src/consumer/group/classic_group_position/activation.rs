//! Pure conversion from one confirmed group position terminal to Fetch policy input.

use kafka_client_core::{
    AssignedTopicPartition, GroupAssignmentPartition, GroupPositionBootstrapTerminal,
    GroupPositionFence, GroupPositionPartitionResult, InstallResolvedAssignment,
    ResolvedAssignedPartition,
};

use crate::protocol::consumer::throttle_ticks;

use super::ClassicGroupPositionCompleted;

/// Local reason a completed position owner cannot become resolved Fetch input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionActivationError {
    FenceMismatch {
        completed: GroupPositionFence,
        current: GroupPositionFence,
    },
    TerminalNotReady,
    PartitionNotCommitted(GroupAssignmentPartition),
    ThrottleOverflow,
    Allocation,
}

/// Copies one exact Ready terminal into deadline-free assigned-consumer input.
///
/// This seam preserves the position terminal's observation moment only for
/// initial broker-throttle policy. A later effect interpreter, not this
/// conversion, owns capture of each internal Fetch attempt deadline.
pub(in crate::consumer::group) fn prepare_classic_group_fetch_activation(
    completed: &ClassicGroupPositionCompleted,
    current: GroupPositionFence,
) -> Result<InstallResolvedAssignment, ClassicGroupPositionActivationError> {
    let completed_fence = completed.fence();
    if completed_fence != current {
        return Err(ClassicGroupPositionActivationError::FenceMismatch {
            completed: completed_fence,
            current,
        });
    }
    let GroupPositionBootstrapTerminal::Ready(batch) = completed.terminal() else {
        return Err(ClassicGroupPositionActivationError::TerminalNotReady);
    };
    let ticks = throttle_ticks(batch.throttle_time_ms())
        .ok_or(ClassicGroupPositionActivationError::ThrottleOverflow)?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(batch.facts().len())
        .map_err(|_error| ClassicGroupPositionActivationError::Allocation)?;
    for fact in batch.facts().iter().copied() {
        let GroupPositionPartitionResult::Committed(next_offset) = fact.result() else {
            return Err(ClassicGroupPositionActivationError::PartitionNotCommitted(
                fact.partition(),
            ));
        };
        let partition = fact.partition();
        partitions.push(ResolvedAssignedPartition::new(
            AssignedTopicPartition::new(partition.topic_id(), partition.partition()),
            next_offset,
        ));
    }
    Ok(InstallResolvedAssignment::new(
        None,
        partitions,
        completed.observed_at(),
        ticks,
    ))
}
