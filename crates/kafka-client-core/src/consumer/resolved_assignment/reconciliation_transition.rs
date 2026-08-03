//! Atomic retained-state movement into one fresh resolved assignment epoch.

use super::{
    ReconcileResolvedAssignment, ReconcileResolvedAssignmentError,
    ReconcileResolvedAssignmentErrorKind, ResolvedAssignmentTarget,
};
use crate::consumer::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, AssignedTopicPartition, AssignmentEpoch, machine::DirectAssignment,
    position::AssignedPartitionState, position_state::RetainedAssignmentPositionPlan,
};
use crate::{Deadline, Moment};

pub(super) enum PreparedReconciliationTarget {
    Retain {
        partition: AssignedTopicPartition,
        plan: RetainedAssignmentPositionPlan,
    },
    Acquire {
        state: AssignedPartitionState,
        effect: AssignedConsumerEffect,
    },
}

struct PreparedReconciliation {
    epoch: AssignmentEpoch,
    next_epoch: AssignmentEpoch,
    targets: Vec<PreparedReconciliationTarget>,
    states: Vec<AssignedPartitionState>,
    effects: Vec<AssignedConsumerEffect>,
}

impl AssignedConsumerMachine {
    /// Reconciles retained and acquired positions into one fresh assignment epoch.
    pub fn reconcile_resolved_assignment(
        &mut self,
        input: ReconcileResolvedAssignment,
    ) -> Result<AssignedConsumerTransition, ReconcileResolvedAssignmentError> {
        let prepared = match self.prepare_reconciliation(&input) {
            Ok(prepared) => prepared,
            Err(kind) => return Err(ReconcileResolvedAssignmentError::new(kind, input)),
        };
        Ok(self.install_prepared_reconciliation(prepared))
    }

    fn prepare_reconciliation(
        &self,
        input: &ReconcileResolvedAssignment,
    ) -> Result<PreparedReconciliation, ReconcileResolvedAssignmentErrorKind> {
        if self.is_closed() {
            return Err(ReconcileResolvedAssignmentErrorKind::ConsumerClosed);
        }
        let actual = self.assignment_epoch();
        if actual != Some(input.expected_assignment_epoch()) {
            return Err(
                ReconcileResolvedAssignmentErrorKind::AssignmentEpochMismatch {
                    expected: input.expected_assignment_epoch(),
                    actual,
                },
            );
        }
        validate_targets(input.targets())?;
        let assignment = self
            .assignment
            .as_ref()
            .unwrap_or_else(|| unreachable!("matching active epoch proves assignment ownership"));
        let epoch = self.next_epoch;
        let next_epoch = epoch
            .checked_next()
            .ok_or(ReconcileResolvedAssignmentErrorKind::AssignmentEpochExhausted)?;
        let acquire_count = input
            .targets()
            .iter()
            .filter(|target| matches!(target, ResolvedAssignmentTarget::Acquire(_)))
            .count();
        let throttle_deadline = acquired_throttle_deadline(input, acquire_count)?;
        let mut targets = Vec::new();
        if !reserve_reconciliation_targets(&mut targets, input.targets().len()) {
            return Err(ReconcileResolvedAssignmentErrorKind::ReconciliationAllocationFailed);
        }
        let mut restart_count = acquire_count;
        for target in input.targets().iter().copied() {
            let (target, restarts) = prepare_reconciliation_target(
                assignment,
                target,
                epoch,
                input.now(),
                throttle_deadline,
            )?;
            restart_count = restart_count
                .checked_add(usize::from(restarts))
                .ok_or(ReconcileResolvedAssignmentErrorKind::ReconciliationAllocationFailed)?;
            targets.push(target);
        }
        let retained_count = input.targets().len().saturating_sub(acquire_count);
        let removed_count = assignment.partitions.len().saturating_sub(retained_count);
        let effect_count = removed_count
            .checked_add(retained_count)
            .and_then(|count| count.checked_add(restart_count))
            .ok_or(ReconcileResolvedAssignmentErrorKind::ReconciliationAllocationFailed)?;
        let mut states = Vec::new();
        let mut effects = Vec::new();
        if !reserve_reconciliation_storage(
            &mut states,
            input.targets().len(),
            &mut effects,
            effect_count,
        ) {
            return Err(ReconcileResolvedAssignmentErrorKind::ReconciliationAllocationFailed);
        }
        effects.extend(
            assignment
                .partitions
                .iter()
                .filter(|state| !target_retains(input.targets(), state.partition))
                .map(|state| AssignedConsumerEffect::Revoke {
                    assignment_epoch: assignment.epoch,
                    partition: state.partition,
                }),
        );
        effects.extend(targets.iter().filter_map(|target| match target {
            PreparedReconciliationTarget::Retain { partition, plan } => {
                Some(AssignedConsumerEffect::Suspend {
                    fence: plan.suspension_fence(assignment.epoch, *partition),
                })
            }
            PreparedReconciliationTarget::Acquire { .. } => None,
        }));
        Ok(PreparedReconciliation {
            epoch,
            next_epoch,
            targets,
            states,
            effects,
        })
    }

    fn install_prepared_reconciliation(
        &mut self,
        mut prepared: PreparedReconciliation,
    ) -> AssignedConsumerTransition {
        let assignment = self
            .assignment
            .take()
            .unwrap_or_else(|| unreachable!("preflighted active assignment remains owned"));
        let mut old_states = assignment.partitions;
        for target in prepared.targets {
            match target {
                PreparedReconciliationTarget::Retain { partition, plan } => {
                    let index = old_states
                        .iter()
                        .position(|state| state.partition == partition)
                        .unwrap_or_else(|| {
                            unreachable!("preflighted retained state remains owned")
                        });
                    let mut state = old_states.swap_remove(index);
                    if let Some(effect) = state.install_assignment_reconciliation(plan) {
                        prepared.effects.push(effect);
                    }
                    prepared.states.push(state);
                }
                PreparedReconciliationTarget::Acquire { state, effect } => {
                    prepared.effects.push(effect);
                    prepared.states.push(state);
                }
            }
        }
        self.assignment = Some(DirectAssignment {
            epoch: prepared.epoch,
            partitions: prepared.states,
        });
        self.next_epoch = prepared.next_epoch;
        AssignedConsumerTransition::new(prepared.epoch, prepared.effects)
    }
}

fn prepare_reconciliation_target(
    assignment: &DirectAssignment,
    target: ResolvedAssignmentTarget,
    epoch: AssignmentEpoch,
    now: Moment,
    throttle_deadline: Option<Deadline>,
) -> Result<(PreparedReconciliationTarget, bool), ReconcileResolvedAssignmentErrorKind> {
    match target {
        ResolvedAssignmentTarget::Retain(partition) => {
            let state = assignment
                .partitions
                .iter()
                .find(|state| state.partition == partition)
                .ok_or(
                    ReconcileResolvedAssignmentErrorKind::RetainedPartitionMissing { partition },
                )?;
            let plan = state
                .plan_assignment_reconciliation(epoch, now)
                .map_err(|error| reconciliation_position_error(error, partition))?;
            let restarts = !state.is_paused() && plan.has_effect();
            Ok((
                PreparedReconciliationTarget::Retain { partition, plan },
                restarts,
            ))
        }
        ResolvedAssignmentTarget::Acquire(assigned) => {
            let partition = assigned.partition();
            if assignment
                .partitions
                .iter()
                .any(|state| state.partition == partition)
            {
                return Err(
                    ReconcileResolvedAssignmentErrorKind::AcquiredPartitionAlreadyExists {
                        partition,
                    },
                );
            }
            let (state, effect) =
                AssignedPartitionState::new_resolved(epoch, assigned, throttle_deadline);
            Ok((
                PreparedReconciliationTarget::Acquire { state, effect },
                false,
            ))
        }
    }
}

fn validate_targets(
    targets: &[ResolvedAssignmentTarget],
) -> Result<(), ReconcileResolvedAssignmentErrorKind> {
    for pair in targets.windows(2) {
        let previous = pair[0].partition();
        let current = pair[1].partition();
        if current == previous {
            return Err(ReconcileResolvedAssignmentErrorKind::DuplicatePartition {
                partition: current,
            });
        }
        if current < previous {
            return Err(ReconcileResolvedAssignmentErrorKind::TargetOutOfOrder {
                previous,
                current,
            });
        }
    }
    Ok(())
}

fn acquired_throttle_deadline(
    input: &ReconcileResolvedAssignment,
    acquire_count: usize,
) -> Result<Option<Deadline>, ReconcileResolvedAssignmentErrorKind> {
    match (acquire_count, input.acquired_throttle_ticks()) {
        (0, _) | (_, 0) => Ok(None),
        (_, ticks) => input
            .now()
            .checked_deadline_after(ticks)
            .map(Some)
            .ok_or(ReconcileResolvedAssignmentErrorKind::AcquiredFetchThrottleDeadlineOverflow),
    }
}

fn target_retains(targets: &[ResolvedAssignmentTarget], partition: AssignedTopicPartition) -> bool {
    targets.iter().any(|target| {
        matches!(target, ResolvedAssignmentTarget::Retain(retained) if *retained == partition)
    })
}

fn reconciliation_position_error(
    error: AssignedConsumerMachineError,
    partition: AssignedTopicPartition,
) -> ReconcileResolvedAssignmentErrorKind {
    match error {
        AssignedConsumerMachineError::PositionEpochExhausted { .. } => {
            ReconcileResolvedAssignmentErrorKind::PositionEpochExhausted { partition }
        }
        _ => unreachable!("reconciliation position preflight only advances its epoch"),
    }
}

pub(super) fn reserve_reconciliation_storage(
    states: &mut Vec<AssignedPartitionState>,
    state_count: usize,
    effects: &mut Vec<AssignedConsumerEffect>,
    effect_count: usize,
) -> bool {
    states.try_reserve_exact(state_count).is_ok() && effects.try_reserve_exact(effect_count).is_ok()
}

pub(super) fn reserve_reconciliation_targets(
    targets: &mut Vec<PreparedReconciliationTarget>,
    target_count: usize,
) -> bool {
    targets.try_reserve_exact(target_count).is_ok()
}
