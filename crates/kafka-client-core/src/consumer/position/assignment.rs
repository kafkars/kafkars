//! Initial unresolved and pre-resolved assigned-partition construction.

use super::AssignedPartitionState;
use crate::{
    Deadline, Moment,
    consumer::{
        AssignedConsumerEffect, AssignedConsumerMachineError, AssignedPartition, AssignmentEpoch,
        ResolvedAssignedPartition, StartPosition, position_state::PartitionPosition,
    },
};

impl AssignedPartitionState {
    pub(in crate::consumer) fn new(
        assignment_epoch: AssignmentEpoch,
        assigned: AssignedPartition,
        now: Moment,
        deadline: Deadline,
    ) -> Result<(Self, AssignedConsumerEffect), AssignedConsumerMachineError> {
        let mut state = Self {
            assignment_epoch,
            partition: assigned.partition(),
            paused: false,
            position: PartitionPosition::new(assigned.start()),
        };
        let effect = state.activate(now, deadline)?.ok_or(
            AssignedConsumerMachineError::PositionResolutionNotPending {
                fence: state.position_fence(),
            },
        )?;
        Ok((state, effect))
    }

    pub(in crate::consumer) fn new_resolved(
        assignment_epoch: AssignmentEpoch,
        assigned: ResolvedAssignedPartition,
        throttle_deadline: Option<Deadline>,
    ) -> (Self, AssignedConsumerEffect) {
        let mut state = Self {
            assignment_epoch,
            partition: assigned.partition(),
            paused: false,
            position: PartitionPosition::new(StartPosition::Offset(assigned.next_offset())),
        };
        let fence = state.position_fence();
        let effect = state.position.start_resolved_assignment_fetch(
            fence,
            assigned.next_offset(),
            throttle_deadline,
        );
        (state, effect)
    }
}
