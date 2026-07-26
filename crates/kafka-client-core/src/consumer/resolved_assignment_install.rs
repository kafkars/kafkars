//! Atomic installation of one complete deadline-free resolved assignment.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerTransition,
    InstallResolvedAssignment, InstallResolvedAssignmentError, InstallResolvedAssignmentErrorKind,
    machine::DirectAssignment, position::AssignedPartitionState,
};

struct PreparedResolvedAssignment {
    epoch: super::AssignmentEpoch,
    next_epoch: super::AssignmentEpoch,
    states: Vec<AssignedPartitionState>,
    effects: Vec<AssignedConsumerEffect>,
}

impl AssignedConsumerMachine {
    /// Installs one ordered explicit-offset assignment without position resolution.
    pub fn install_resolved_assignment(
        &mut self,
        input: InstallResolvedAssignment,
    ) -> Result<AssignedConsumerTransition, InstallResolvedAssignmentError> {
        let result = self.prepare_resolved_assignment(&input);
        let prepared = match result {
            Ok(prepared) => prepared,
            Err(kind) => return Err(InstallResolvedAssignmentError::new(kind, input)),
        };

        self.assignment = Some(DirectAssignment {
            epoch: prepared.epoch,
            partitions: prepared.states,
        });
        self.next_epoch = prepared.next_epoch;
        Ok(AssignedConsumerTransition::new(
            prepared.epoch,
            prepared.effects,
        ))
    }

    fn prepare_resolved_assignment(
        &self,
        input: &InstallResolvedAssignment,
    ) -> Result<PreparedResolvedAssignment, InstallResolvedAssignmentErrorKind> {
        if self.is_closed() {
            return Err(InstallResolvedAssignmentErrorKind::ConsumerClosed);
        }
        let actual_epoch = self.assignment_epoch();
        if input.expected_assignment_epoch() != actual_epoch {
            return Err(
                InstallResolvedAssignmentErrorKind::ResolvedAssignmentEpochMismatch {
                    expected: input.expected_assignment_epoch(),
                    actual: actual_epoch,
                },
            );
        }
        validate_resolved_partitions(input.partitions())?;
        let epoch = self.next_epoch;
        let next_epoch = epoch
            .checked_next()
            .ok_or(InstallResolvedAssignmentErrorKind::AssignmentEpochExhausted)?;
        let throttle_deadline =
            match (input.partitions().is_empty(), input.throttle_ticks()) {
                (true, _) | (false, 0) => None,
                (false, ticks) => Some(input.now().checked_deadline_after(ticks).ok_or(
                    InstallResolvedAssignmentErrorKind::InitialFetchThrottleDeadlineOverflow,
                )?),
            };
        let old_count = self
            .assignment
            .as_ref()
            .map_or(0, |assignment| assignment.partitions.len());
        let effect_count = old_count
            .checked_add(input.partitions().len())
            .ok_or(InstallResolvedAssignmentErrorKind::AssignmentAllocationFailed)?;
        let mut states = Vec::new();
        let mut effects = Vec::new();
        if !reserve_resolved_assignment_storage(
            &mut states,
            input.partitions().len(),
            &mut effects,
            effect_count,
        ) {
            return Err(InstallResolvedAssignmentErrorKind::AssignmentAllocationFailed);
        }
        if let Some(assignment) = &self.assignment {
            effects.extend(assignment.partitions.iter().map(|state| {
                AssignedConsumerEffect::Revoke {
                    assignment_epoch: assignment.epoch,
                    partition: state.partition,
                }
            }));
        }
        for assigned in input.partitions().iter().copied() {
            let (state, effect) =
                AssignedPartitionState::new_resolved(epoch, assigned, throttle_deadline);
            states.push(state);
            effects.push(effect);
        }
        Ok(PreparedResolvedAssignment {
            epoch,
            next_epoch,
            states,
            effects,
        })
    }
}

fn validate_resolved_partitions(
    partitions: &[super::ResolvedAssignedPartition],
) -> Result<(), InstallResolvedAssignmentErrorKind> {
    for pair in partitions.windows(2) {
        let previous = pair[0].partition();
        let current = pair[1].partition();
        if current == previous {
            return Err(InstallResolvedAssignmentErrorKind::DuplicatePartition {
                partition: current,
            });
        }
        if current < previous {
            return Err(
                InstallResolvedAssignmentErrorKind::ResolvedAssignmentOutOfOrder {
                    previous,
                    current,
                },
            );
        }
    }
    Ok(())
}

pub(super) fn reserve_resolved_assignment_storage(
    states: &mut Vec<AssignedPartitionState>,
    state_count: usize,
    effects: &mut Vec<AssignedConsumerEffect>,
    effect_count: usize,
) -> bool {
    states.try_reserve_exact(state_count).is_ok() && effects.try_reserve_exact(effect_count).is_ok()
}
