//! Atomic retained Fetch-state replacement from one confirmed cooperative position batch.

use kafka_client_core::{
    AssignedConsumerTransition, AssignedTopicPartition, GroupAssignmentPartition,
    GroupPositionBootstrapTerminal, GroupPositionFence, GroupPositionPartitionResult,
    ReconcileResolvedAssignment, ReconcileResolvedAssignmentErrorKind, ResolvedAssignedPartition,
    ResolvedAssignmentTarget,
};

use crate::{
    consumer::assigned_event::AssignedConsumerEventStoreError, protocol::consumer::throttle_ticks,
};

use super::{
    activation::{ClassicGroupFetchActivation, ClassicGroupFetchBinding},
    owner::ClassicGroupFetchOwner,
};
use crate::consumer::group::classic_group_position::ClassicGroupPositionCompleted;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchReconciliationErrorKind {
    NotReady,
    BindingMismatch,
    PositionShape,
    Allocation,
    EffectCapacity,
    Event(AssignedConsumerEventStoreError),
    Core(ReconcileResolvedAssignmentErrorKind),
    PostCore,
}

#[must_use = "a rejected Fetch reconciliation retains its exact completed position owner"]
pub(in crate::consumer::group) enum ClassicGroupFetchReconciliationError {
    Returned {
        kind: ClassicGroupFetchReconciliationErrorKind,
        completed: ClassicGroupPositionCompleted,
    },
    Retained(ClassicGroupFetchReconciliationErrorKind),
}

impl ClassicGroupFetchReconciliationError {
    pub(in crate::consumer::group) const fn kind(
        &self,
    ) -> ClassicGroupFetchReconciliationErrorKind {
        match self {
            Self::Returned { kind, .. } | Self::Retained(kind) => *kind,
        }
    }

    pub(in crate::consumer::group) fn into_completed(
        self,
    ) -> Option<ClassicGroupPositionCompleted> {
        match self {
            Self::Returned { completed, .. } => Some(completed),
            Self::Retained(_) => None,
        }
    }
}

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) fn reconciliation_is_ready(&self) -> bool {
        !self.is_faulted()
            && self.effects.is_empty()
            && self.seek.is_none()
            && self.raw_position_deadlines.is_empty()
            && self.pending_positions.is_empty()
            && self.positions.retained_positions() == 0
            && !self.has_ready_delivery()
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the reconciliation boundary names both exact fences and every ordered ownership set"
    )]
    #[expect(
        clippy::result_large_err,
        reason = "local rejection must return the exact completed position owner without boxing"
    )]
    pub(in crate::consumer::group) fn reconcile_assignment(
        &mut self,
        completed: ClassicGroupPositionCompleted,
        previous_fence: GroupPositionFence,
        replacement_fence: GroupPositionFence,
        retained: &[GroupAssignmentPartition],
        removed: &[GroupAssignmentPartition],
        added: &[GroupAssignmentPartition],
        replacement: &[GroupAssignmentPartition],
    ) -> Result<(), ClassicGroupFetchReconciliationError> {
        if !self.reconciliation_is_ready() {
            return returned(NotReady, completed);
        }
        let Some(activation) = self.activation.as_ref() else {
            return returned(BindingMismatch, completed);
        };
        if activation.binding().position_fence() != previous_fence
            || self.machine.assignment_epoch() != Some(activation.binding().assignment_epoch())
            || completed.fence() != replacement_fence
        {
            return returned(BindingMismatch, completed);
        }
        let input = match prepare_input(
            &completed,
            activation.binding().assignment_epoch(),
            retained,
            added,
            replacement,
        ) {
            Ok(input) => input,
            Err(kind) => return returned(kind, completed),
        };
        let maximum_effects = removed
            .len()
            .saturating_add(retained.len().saturating_mul(2))
            .saturating_add(added.len());
        if self.effects.len().saturating_add(maximum_effects) > self.effect_capacity {
            return returned(EffectCapacity, completed);
        }
        let event_claims = match self.events.prepare_reconciliation(replacement.len()) {
            Ok(claims) => claims,
            Err(error) => return returned(Event(error), completed),
        };
        let transition = match self.machine.reconcile_resolved_assignment(input) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return returned(Core(error.kind()), completed);
            }
        };
        let Some(assignment_epoch) = transition.assignment_epoch() else {
            event_claims.rollback_event_claims();
            self.retain_reconciliation_fault(completed, transition, PostCore);
            return Err(ClassicGroupFetchReconciliationError::Retained(PostCore));
        };
        if transition.effects().len() > self.effect_capacity.saturating_sub(self.effects.len()) {
            event_claims.rollback_event_claims();
            self.retain_reconciliation_fault(completed, transition, EffectCapacity);
            return Err(ClassicGroupFetchReconciliationError::Retained(
                EffectCapacity,
            ));
        }
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            self.retain_reconciliation_fault(completed, transition, Event(error));
            return Err(ClassicGroupFetchReconciliationError::Retained(Event(error)));
        }
        self.activation = Some(ClassicGroupFetchActivation::new(
            ClassicGroupFetchBinding::new(replacement_fence, assignment_epoch),
        ));
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        self.fetches.resume_broker_session_maintenance();
        Ok(())
    }

    fn retain_reconciliation_fault(
        &mut self,
        completed: ClassicGroupPositionCompleted,
        transition: AssignedConsumerTransition,
        kind: ClassicGroupFetchReconciliationErrorKind,
    ) {
        self.fault = Some(super::model::ClassicGroupFetchOwnerFault::Reconciliation {
            _completed: completed,
            transition,
            kind,
        });
    }
}

fn prepare_input(
    completed: &ClassicGroupPositionCompleted,
    assignment_epoch: kafka_client_core::AssignmentEpoch,
    retained: &[GroupAssignmentPartition],
    added: &[GroupAssignmentPartition],
    replacement: &[GroupAssignmentPartition],
) -> Result<ReconcileResolvedAssignment, ClassicGroupFetchReconciliationErrorKind> {
    let GroupPositionBootstrapTerminal::Ready(batch) = completed.terminal() else {
        return Err(PositionShape);
    };
    if batch.facts().len() != added.len() {
        return Err(PositionShape);
    }
    let ticks = throttle_ticks(batch.throttle_time_ms()).ok_or(PositionShape)?;
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(replacement.len())
        .map_err(|_error| Allocation)?;
    for partition in replacement {
        let assigned = AssignedTopicPartition::new(partition.topic_id(), partition.partition());
        if retained.binary_search(partition).is_ok() {
            targets.push(ResolvedAssignmentTarget::Retain(assigned));
            continue;
        }
        if added.binary_search(partition).is_err() {
            return Err(PositionShape);
        }
        let fact = batch
            .facts()
            .iter()
            .find(|fact| fact.partition() == *partition)
            .ok_or(PositionShape)?;
        let GroupPositionPartitionResult::Committed(next_offset) = fact.result() else {
            return Err(PositionShape);
        };
        targets.push(ResolvedAssignmentTarget::Acquire(
            ResolvedAssignedPartition::new(assigned, next_offset),
        ));
    }
    Ok(ReconcileResolvedAssignment::new(
        assignment_epoch,
        targets,
        completed.observed_at(),
        ticks,
    ))
}

#[expect(
    clippy::result_large_err,
    reason = "the lossless helper returns the exact completed position owner"
)]
fn returned(
    kind: ClassicGroupFetchReconciliationErrorKind,
    completed: ClassicGroupPositionCompleted,
) -> Result<(), ClassicGroupFetchReconciliationError> {
    Err(ClassicGroupFetchReconciliationError::Returned { kind, completed })
}

use ClassicGroupFetchReconciliationErrorKind::{
    Allocation, BindingMismatch, Core, EffectCapacity, Event, NotReady, PositionShape, PostCore,
};
