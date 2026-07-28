//! One pre-reserved terminal seek bound to the active group assignment.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedTopicPartition, GroupPositionFence,
    PositionFence, StartPosition,
};

use crate::{
    clock::DeadlineCapture,
    consumer::{
        assigned_owner_model::RawPositionDeadline,
        group_seek::{
            GroupConsumerSeekCompletion, GroupConsumerSeekTerminal,
            GroupConsumerSeekTerminalFailure, GroupConsumerSeekTerminalFailureKind,
        },
    },
};

use super::{
    model::{ClassicGroupFetchOwnerFault, ClassicGroupFetchTransitionFailure},
    owner::ClassicGroupFetchOwner,
    seek_terminal::seek_terminal,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchSeekError {
    Faulted,
    Inactive,
    BindingMismatch,
    Pending,
    Capacity,
    UnknownPartition,
    Core,
}

pub(in crate::consumer::group) enum ClassicGroupFetchSeekObservation {
    Pending,
    TerminalMissing,
}

pub(super) struct ClassicGroupFetchSeek {
    pub(super) fence: PositionFence,
    completion: Arc<GroupConsumerSeekCompletion>,
}

impl ClassicGroupFetchSeek {
    pub(super) fn matches(&self, completion: &Arc<GroupConsumerSeekCompletion>) -> bool {
        Arc::ptr_eq(&self.completion, completion)
    }
}

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) fn seek_partition(
        &mut self,
        position_fence: GroupPositionFence,
        partition: AssignedTopicPartition,
        position: StartPosition,
        capture: DeadlineCapture,
        completion: Arc<GroupConsumerSeekCompletion>,
    ) -> Result<(), ClassicGroupFetchSeekError> {
        let assignment_epoch = self.preflight_seek(position_fence)?;
        let claims = self
            .events
            .prepare_partition(partition)
            .map_err(|_| ClassicGroupFetchSeekError::Capacity)?;
        let input = AssignedConsumerInput::Seek {
            assignment_epoch,
            partition,
            position,
            now: capture.now(),
            resolution_deadline: capture.deadline(),
        };
        let transition = match self.machine.apply(input) {
            Ok(transition) => transition,
            Err(kafka_client_core::AssignedConsumerMachineError::UnknownPartition { .. }) => {
                claims.rollback_event_claims();
                return Err(ClassicGroupFetchSeekError::UnknownPartition);
            }
            Err(_error) => {
                claims.rollback_event_claims();
                return Err(ClassicGroupFetchSeekError::Core);
            }
        };
        if transition.assignment_epoch() != Some(assignment_epoch)
            || transition.effects().is_empty()
            || transition.effects().len() > 2
        {
            claims.rollback_event_claims();
            self.retain_failed_seek_transition(transition, &completion);
            return Ok(());
        }
        if claims.commit_event_claims(transition.effects()).is_err() {
            self.retain_failed_seek_transition(transition, &completion);
            return Ok(());
        }
        let pending = transition.effects().iter().find_map(|effect| match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => Some(*fence),
            _ => None,
        });
        if let Some(fence) = pending {
            self.raw_position_deadlines.push_back(RawPositionDeadline {
                fence,
                deadline: capture.operation_deadline(),
            });
            self.seek = Some(ClassicGroupFetchSeek { fence, completion });
        } else {
            let _published = completion.publish(GroupConsumerSeekTerminal::Succeeded);
        }
        self.effects.extend(transition.into_effects());
        Ok(())
    }

    pub(in crate::consumer::group) fn seek_observation(
        &self,
        completion: &Arc<GroupConsumerSeekCompletion>,
    ) -> ClassicGroupFetchSeekObservation {
        match &self.seek {
            Some(seek) if seek.matches(completion) => ClassicGroupFetchSeekObservation::Pending,
            _ => ClassicGroupFetchSeekObservation::TerminalMissing,
        }
    }

    pub(in crate::consumer::group) const fn is_faulted_for_seek(&self) -> bool {
        self.is_faulted()
    }

    pub(super) fn settle_seek_transition(
        &mut self,
        transition: &kafka_client_core::AssignedConsumerTransition,
    ) {
        let Some(seek) = self.seek.as_ref() else {
            return;
        };
        let terminal = transition
            .effects()
            .iter()
            .find_map(|effect| seek_terminal(seek.fence, *effect));
        let Some(terminal) = terminal else {
            return;
        };
        let seek = self.seek.take().unwrap_or_else(|| unreachable!());
        let _published = seek.completion.publish(terminal);
    }

    pub(super) fn settle_seek_assignment_lost(&mut self) {
        self.settle_seek_failure(GroupConsumerSeekTerminalFailureKind::AssignmentLost, None);
    }

    pub(super) fn settle_seek_host_unavailable(&mut self) {
        self.settle_seek_failure(GroupConsumerSeekTerminalFailureKind::HostUnavailable, None);
    }

    pub(super) fn settle_seek_driver_shutdown(&mut self) {
        self.settle_seek_failure(GroupConsumerSeekTerminalFailureKind::Transport, None);
    }

    fn preflight_seek(
        &self,
        position_fence: GroupPositionFence,
    ) -> Result<kafka_client_core::AssignmentEpoch, ClassicGroupFetchSeekError> {
        if self.is_faulted() {
            return Err(ClassicGroupFetchSeekError::Faulted);
        }
        let Some(activation) = self.activation.as_ref() else {
            return Err(ClassicGroupFetchSeekError::Inactive);
        };
        let binding = activation.binding();
        if binding.position_fence() != position_fence
            || self.machine.assignment_epoch() != Some(binding.assignment_epoch())
        {
            return Err(ClassicGroupFetchSeekError::BindingMismatch);
        }
        if !self.effects.is_empty() || self.seek.is_some() {
            return Err(ClassicGroupFetchSeekError::Pending);
        }
        if self.effects.len().saturating_add(2) > self.effect_capacity
            || self.raw_position_deadlines.len() >= self.partition_capacity
            || self.pending_positions.len() >= self.partition_capacity
        {
            return Err(ClassicGroupFetchSeekError::Capacity);
        }
        Ok(binding.assignment_epoch())
    }

    fn retain_failed_seek_transition(
        &mut self,
        transition: kafka_client_core::AssignedConsumerTransition,
        completion: &GroupConsumerSeekCompletion,
    ) {
        let failure = ClassicGroupFetchTransitionFailure::ControlInvariant;
        let _published = completion.publish(failed(
            GroupConsumerSeekTerminalFailureKind::InternalInvariant,
            None,
        ));
        self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
            _transition: transition,
            failure,
        });
    }

    fn settle_seek_failure(
        &mut self,
        kind: GroupConsumerSeekTerminalFailureKind,
        broker_code: Option<i16>,
    ) {
        let Some(seek) = self.seek.take() else {
            return;
        };
        let _published = seek.completion.publish(failed(kind, broker_code));
    }
}

const fn failed(
    kind: GroupConsumerSeekTerminalFailureKind,
    broker_code: Option<i16>,
) -> GroupConsumerSeekTerminal {
    GroupConsumerSeekTerminal::Failed(GroupConsumerSeekTerminalFailure { kind, broker_code })
}
