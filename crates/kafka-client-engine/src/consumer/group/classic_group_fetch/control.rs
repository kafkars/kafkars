//! Atomic assignment-bound batch control over one active classic-group Fetch owner.

use kafka_client_core::{
    AssignedConsumerMachineError, AssignedTopicPartition, AssignmentEpoch, GroupPositionFence,
};

use crate::{clock::DeadlineCapture, consumer::assigned_owner_model::RawPositionDeadline};

use super::{
    model::{ClassicGroupFetchOwnerFault, ClassicGroupFetchTransitionFailure},
    owner::ClassicGroupFetchOwner,
};

/// Pre-core rejection of one classic-group Fetch control batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ClassicGroupFetchControlError {
    Faulted,
    Inactive,
    BindingMismatch,
    Pending,
    EffectCapacity,
    Event(crate::consumer::assigned_event::AssignedConsumerEventStoreError),
    Core(AssignedConsumerMachineError),
}

/// Accepted deterministic control progress retained by the Fetch owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) struct ClassicGroupFetchControlAccepted {
    effects: usize,
    retained_fault: bool,
}

impl ClassicGroupFetchControlAccepted {
    const fn accepted(effects: usize) -> Self {
        Self {
            effects,
            retained_fault: false,
        }
    }

    const fn retained_fault() -> Self {
        Self {
            effects: 0,
            retained_fault: true,
        }
    }

    pub(in crate::consumer::group) const fn effects(self) -> usize {
        self.effects
    }

    pub(in crate::consumer::group) const fn fault_retained(self) -> bool {
        self.retained_fault
    }
}

impl ClassicGroupFetchOwner {
    /// Pauses every unique caller-ordered target after complete owner preflight.
    pub(in crate::consumer::group) fn pause_partitions(
        &mut self,
        position_fence: GroupPositionFence,
        partitions: &[AssignedTopicPartition],
    ) -> Result<ClassicGroupFetchControlAccepted, ClassicGroupFetchControlError> {
        let assignment_epoch = self.preflight_control(position_fence, partitions.len())?;
        let event_claims = self
            .events
            .prepare_pause_partitions(partitions)
            .map_err(ClassicGroupFetchControlError::Event)?;
        let transition = match self.machine.pause_partitions(assignment_epoch, partitions) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(ClassicGroupFetchControlError::Core(error));
            }
        };
        if transition.assignment_epoch() != Some(assignment_epoch)
            || transition.effects().len() > partitions.len()
        {
            event_claims.rollback_event_claims();
            self.retain_control_fault(transition);
            return Ok(ClassicGroupFetchControlAccepted::retained_fault());
        }
        if event_claims
            .commit_event_claims(transition.effects())
            .is_err()
        {
            self.retain_control_fault(transition);
            return Ok(ClassicGroupFetchControlAccepted::retained_fault());
        }
        Ok(self.append_control_transition(transition))
    }

    /// Resumes retained group positions using the outer-boundary time unchanged.
    pub(in crate::consumer::group) fn resume_partitions(
        &mut self,
        position_fence: GroupPositionFence,
        partitions: &[AssignedTopicPartition],
        capture: DeadlineCapture,
    ) -> Result<ClassicGroupFetchControlAccepted, ClassicGroupFetchControlError> {
        let assignment_epoch = self.preflight_control(position_fence, partitions.len())?;
        if self
            .raw_position_deadlines
            .len()
            .saturating_add(partitions.len())
            > self.partition_capacity
        {
            return Err(ClassicGroupFetchControlError::EffectCapacity);
        }
        let event_claims = self
            .events
            .prepare_resume_partitions(partitions)
            .map_err(ClassicGroupFetchControlError::Event)?;
        let transition = match self.machine.resume_retained_partitions(
            assignment_epoch,
            partitions,
            capture.now(),
            capture.deadline(),
        ) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(ClassicGroupFetchControlError::Core(error));
            }
        };
        if transition.assignment_epoch() != Some(assignment_epoch)
            || transition.effects().len() > partitions.len()
        {
            event_claims.rollback_event_claims();
            self.retain_control_fault(transition);
            return Ok(ClassicGroupFetchControlAccepted::retained_fault());
        }
        if event_claims
            .commit_event_claims(transition.effects())
            .is_err()
        {
            self.retain_control_fault(transition);
            return Ok(ClassicGroupFetchControlAccepted::retained_fault());
        }
        for effect in transition.effects() {
            if let kafka_client_core::AssignedConsumerEffect::ResolvePosition { fence, .. } = effect
            {
                self.raw_position_deadlines.push_back(RawPositionDeadline {
                    fence: *fence,
                    deadline: capture.operation_deadline(),
                });
            }
        }
        Ok(self.append_control_transition(transition))
    }

    fn preflight_control(
        &self,
        position_fence: GroupPositionFence,
        maximum_effects: usize,
    ) -> Result<AssignmentEpoch, ClassicGroupFetchControlError> {
        if self.is_faulted() {
            return Err(ClassicGroupFetchControlError::Faulted);
        }
        let Some(activation) = self.activation.as_ref() else {
            return Err(ClassicGroupFetchControlError::Inactive);
        };
        let binding = activation.binding();
        let assignment_epoch = binding.assignment_epoch();
        if binding.position_fence() != position_fence
            || self.machine.assignment_epoch() != Some(assignment_epoch)
        {
            return Err(ClassicGroupFetchControlError::BindingMismatch);
        }
        if !self.effects.is_empty() || self.seek.is_some() {
            return Err(ClassicGroupFetchControlError::Pending);
        }
        if self.effects.len().saturating_add(maximum_effects) > self.effect_capacity {
            return Err(ClassicGroupFetchControlError::EffectCapacity);
        }
        Ok(assignment_epoch)
    }

    fn append_control_transition(
        &mut self,
        transition: kafka_client_core::AssignedConsumerTransition,
    ) -> ClassicGroupFetchControlAccepted {
        let effects = transition.effects().len();
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        ClassicGroupFetchControlAccepted::accepted(effects)
    }

    fn retain_control_fault(&mut self, transition: kafka_client_core::AssignedConsumerTransition) {
        self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
            _transition: transition,
            failure: ClassicGroupFetchTransitionFailure::ControlInvariant,
        });
    }
}
