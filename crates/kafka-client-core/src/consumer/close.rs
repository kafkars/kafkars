//! Core-owned direct-consumer close acceptance, fencing, and completion.

use super::{
    AssignedConsumerCloseId, AssignedConsumerEffect, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedConsumerTransition, AssignmentEpoch, PositionEpoch,
};

const EFFECTS_PER_PARTITION: usize = 2;

#[derive(Debug)]
pub(super) enum AssignedConsumerCloseState {
    Open,
    Draining(AssignedConsumerCloseId),
    Complete(AssignedConsumerCloseId),
}

impl AssignedConsumerCloseState {
    pub(super) const fn is_closed(&self) -> bool {
        !matches!(self, Self::Open)
    }
}

impl AssignedConsumerMachine {
    pub(super) fn begin_close(
        &mut self,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        if self.close_state.is_closed() {
            return Err(AssignedConsumerMachineError::ConsumerClosed);
        }
        let assignment_epoch = self.assignment.as_ref().map(|assignment| assignment.epoch);
        let plans = self
            .assignment
            .as_ref()
            .map(|assignment| {
                assignment
                    .partitions
                    .iter()
                    .map(super::position::AssignedPartitionState::plan_close)
                    .collect::<Result<Vec<PositionEpoch>, AssignedConsumerMachineError>>()
            })
            .transpose()?
            .unwrap_or_default();
        let close_id = AssignedConsumerCloseId::initial();
        let mut effects = Vec::with_capacity(
            plans
                .len()
                .saturating_mul(EFFECTS_PER_PARTITION)
                .saturating_add(1),
        );
        effects.push(AssignedConsumerEffect::AcceptClose { close_id });
        if let Some(assignment) = self.assignment.as_mut() {
            append_cleanup_effects(assignment, plans, &mut effects);
        }
        self.close_state = AssignedConsumerCloseState::Draining(close_id);
        Ok(close_transition(assignment_epoch, effects))
    }

    pub(super) fn close_drained(
        &mut self,
        supplied: AssignedConsumerCloseId,
    ) -> Result<AssignedConsumerTransition, AssignedConsumerMachineError> {
        match &self.close_state {
            AssignedConsumerCloseState::Open => {
                return Err(AssignedConsumerMachineError::CloseNotPending { supplied });
            }
            AssignedConsumerCloseState::Draining(active) if *active != supplied => {
                return Err(AssignedConsumerMachineError::StaleClose {
                    active: *active,
                    supplied,
                });
            }
            AssignedConsumerCloseState::Complete(close_id) if *close_id == supplied => {
                return Err(AssignedConsumerMachineError::CloseAlreadyCompleted {
                    close_id: *close_id,
                });
            }
            AssignedConsumerCloseState::Complete(active) => {
                return Err(AssignedConsumerMachineError::StaleClose {
                    active: *active,
                    supplied,
                });
            }
            AssignedConsumerCloseState::Draining(_) => {}
        }
        self.close_state = AssignedConsumerCloseState::Complete(supplied);
        Ok(close_transition(
            self.assignment.as_ref().map(|assignment| assignment.epoch),
            vec![AssignedConsumerEffect::CompleteClose { close_id: supplied }],
        ))
    }
}

fn append_cleanup_effects(
    assignment: &mut super::machine::DirectAssignment,
    plans: Vec<PositionEpoch>,
    effects: &mut Vec<AssignedConsumerEffect>,
) {
    for (state, next_epoch) in assignment.partitions.iter_mut().zip(plans) {
        let assignment_epoch = state.assignment_epoch();
        effects.push(state.suspend_for_close(next_epoch));
        effects.push(AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition: state.partition,
        });
    }
}

fn close_transition(
    assignment_epoch: Option<AssignmentEpoch>,
    effects: Vec<AssignedConsumerEffect>,
) -> AssignedConsumerTransition {
    match assignment_epoch {
        Some(epoch) => AssignedConsumerTransition::new(epoch, effects),
        None => AssignedConsumerTransition::without_assignment(effects),
    }
}
