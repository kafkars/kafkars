//! Atomic consumer-group member-removal transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    ConsumerGroupMemberRemovalOutcome, RemoveConsumerGroupMembersBatch,
    RemoveConsumerGroupMembersEffect, RemoveConsumerGroupMembersFailure,
    RemoveConsumerGroupMembersFailureKind, RemoveConsumerGroupMembersInput,
    RemoveConsumerGroupMembersMachine, RemoveConsumerGroupMembersMachineError,
    RemoveConsumerGroupMembersState, RemoveConsumerGroupMembersTerminal,
    RemoveConsumerGroupMembersTransition,
};

impl RemoveConsumerGroupMembersMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: RemoveConsumerGroupMembersInput,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state == RemoveConsumerGroupMembersState::Completed {
            return Err(RemoveConsumerGroupMembersMachineError::AlreadyCompleted);
        }
        match input {
            RemoveConsumerGroupMembersInput::Start { now } => self.start(now),
            RemoveConsumerGroupMembersInput::DriverAccepted => self.driver_accepted(),
            RemoveConsumerGroupMembersInput::DriverRejected => self.finish_awaiting(
                RemoveConsumerGroupMembersFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            RemoveConsumerGroupMembersInput::DeadlineElapsed => self.finish_awaiting(
                RemoveConsumerGroupMembersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            RemoveConsumerGroupMembersInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    RemoveConsumerGroupMembersFailureKind::DeadlineElapsed,
                    delivery,
                ),
            RemoveConsumerGroupMembersInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            RemoveConsumerGroupMembersInput::BrokerRejected { code } => self.finish_submitted(
                RemoveConsumerGroupMembersFailureKind::Broker(code),
                DeliveryStatus::PossiblySent,
            ),
            RemoveConsumerGroupMembersInput::ResponseTooLarge => self.finish_submitted(
                RemoveConsumerGroupMembersFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            RemoveConsumerGroupMembersInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    RemoveConsumerGroupMembersFailureKind::Compatibility,
                    delivery,
                ),
            RemoveConsumerGroupMembersInput::TransportFailed { delivery } => {
                self.finish_submitted(RemoveConsumerGroupMembersFailureKind::Transport, delivery)
            }
            RemoveConsumerGroupMembersInput::InvalidResponse => self.finish_submitted(
                RemoveConsumerGroupMembersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state != RemoveConsumerGroupMembersState::Ready {
            return Err(RemoveConsumerGroupMembersMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                RemoveConsumerGroupMembersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = RemoveConsumerGroupMembersState::AwaitingDriver;
        Ok(RemoveConsumerGroupMembersTransition::one(
            RemoveConsumerGroupMembersEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state != RemoveConsumerGroupMembersState::AwaitingDriver {
            return Err(RemoveConsumerGroupMembersMachineError::InvalidState);
        }
        self.state = RemoveConsumerGroupMembersState::Submitted;
        Ok(RemoveConsumerGroupMembersTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: RemoveConsumerGroupMembersBatch,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state != RemoveConsumerGroupMembersState::Submitted {
            return Err(RemoveConsumerGroupMembersMachineError::InvalidState);
        }
        if !self.outcomes_match_plan(batch.outcomes()) {
            return Ok(self.finish_failure(
                RemoveConsumerGroupMembersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(RemoveConsumerGroupMembersTerminal::Removed(batch)))
    }

    fn outcomes_match_plan(&self, outcomes: &[ConsumerGroupMemberRemovalOutcome]) -> bool {
        self.plan.members().len() == outcomes.len()
            && self
                .plan
                .members()
                .iter()
                .zip(outcomes)
                .all(|(member, outcome)| member.group_instance_id() == outcome.group_instance_id())
    }

    fn finish_awaiting(
        &mut self,
        kind: RemoveConsumerGroupMembersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state != RemoveConsumerGroupMembersState::AwaitingDriver {
            return Err(RemoveConsumerGroupMembersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: RemoveConsumerGroupMembersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RemoveConsumerGroupMembersTransition, RemoveConsumerGroupMembersMachineError> {
        if self.state != RemoveConsumerGroupMembersState::Submitted {
            return Err(RemoveConsumerGroupMembersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: RemoveConsumerGroupMembersFailureKind,
        delivery: DeliveryStatus,
    ) -> RemoveConsumerGroupMembersTransition {
        self.finish(RemoveConsumerGroupMembersTerminal::Failed(
            RemoveConsumerGroupMembersFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: RemoveConsumerGroupMembersTerminal,
    ) -> RemoveConsumerGroupMembersTransition {
        self.state = RemoveConsumerGroupMembersState::Completed;
        RemoveConsumerGroupMembersTransition::one(RemoveConsumerGroupMembersEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
