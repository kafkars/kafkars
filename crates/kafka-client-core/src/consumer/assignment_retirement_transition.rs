//! Atomic retirement of one exact active Fetch assignment.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerTransition, RetireAssignment,
    RetireAssignmentError, RetireAssignmentErrorKind,
};

impl AssignedConsumerMachine {
    /// Retires one exact optional assignment without starting a deadline or replacement epoch.
    pub fn retire_assignment(
        &mut self,
        input: RetireAssignment,
    ) -> Result<AssignedConsumerTransition, RetireAssignmentError> {
        let result = self.prepare_assignment_retirement(&input);
        let effects = match result {
            Ok(effects) => effects,
            Err(kind) => return Err(RetireAssignmentError::new(kind, input)),
        };

        if self.assignment.is_some() {
            self.assignment = None;
        }
        Ok(AssignedConsumerTransition::without_assignment(effects))
    }

    fn prepare_assignment_retirement(
        &self,
        input: &RetireAssignment,
    ) -> Result<Vec<AssignedConsumerEffect>, RetireAssignmentErrorKind> {
        if self.is_closed() {
            return Err(RetireAssignmentErrorKind::ConsumerClosed);
        }
        let actual_epoch = self.assignment_epoch();
        if input.expected_assignment_epoch() != actual_epoch {
            return Err(RetireAssignmentErrorKind::AssignmentEpochMismatch {
                expected: input.expected_assignment_epoch(),
                actual: actual_epoch,
            });
        }
        let Some(assignment) = &self.assignment else {
            return Ok(Vec::new());
        };
        let mut effects = Vec::new();
        if !reserve_retirement_effects(&mut effects, assignment.partitions.len()) {
            return Err(RetireAssignmentErrorKind::EffectAllocationFailed);
        }
        effects.extend(
            assignment
                .partitions
                .iter()
                .map(|state| AssignedConsumerEffect::Revoke {
                    assignment_epoch: assignment.epoch,
                    partition: state.partition,
                }),
        );
        Ok(effects)
    }
}

pub(super) fn reserve_retirement_effects(
    effects: &mut Vec<AssignedConsumerEffect>,
    count: usize,
) -> bool {
    effects.try_reserve_exact(count).is_ok()
}
