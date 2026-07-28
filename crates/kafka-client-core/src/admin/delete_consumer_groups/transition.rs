//! Atomic Admin `DeleteConsumerGroups` transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DeleteConsumerGroupsBatch, DeleteConsumerGroupsEffect, DeleteConsumerGroupsFailure,
    DeleteConsumerGroupsFailureKind, DeleteConsumerGroupsInput, DeleteConsumerGroupsMachine,
    DeleteConsumerGroupsMachineError, DeleteConsumerGroupsState, DeleteConsumerGroupsTerminal,
    DeleteConsumerGroupsTransition,
};

impl DeleteConsumerGroupsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DeleteConsumerGroupsInput,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state == DeleteConsumerGroupsState::Completed {
            return Err(DeleteConsumerGroupsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteConsumerGroupsInput::Start { now } => self.start(now),
            DeleteConsumerGroupsInput::DriverAccepted => self.driver_accepted(),
            DeleteConsumerGroupsInput::DriverRejected => self.finish_awaiting(
                DeleteConsumerGroupsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DeleteConsumerGroupsInput::DeadlineElapsed => self.finish_awaiting(
                DeleteConsumerGroupsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DeleteConsumerGroupsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DeleteConsumerGroupsFailureKind::DeadlineElapsed, delivery)
            }
            DeleteConsumerGroupsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            DeleteConsumerGroupsInput::ResponseTooLarge => self.finish_submitted(
                DeleteConsumerGroupsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DeleteConsumerGroupsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DeleteConsumerGroupsFailureKind::Compatibility, delivery)
            }
            DeleteConsumerGroupsInput::TransportFailed { delivery } => {
                self.finish_submitted(DeleteConsumerGroupsFailureKind::Transport, delivery)
            }
            DeleteConsumerGroupsInput::InvalidResponse => self.finish_submitted(
                DeleteConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state != DeleteConsumerGroupsState::Ready {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return self.finish_failure(
                DeleteConsumerGroupsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            );
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        let Some(target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        };
        self.state = DeleteConsumerGroupsState::AwaitingDriver;
        Ok(DeleteConsumerGroupsTransition::one(
            DeleteConsumerGroupsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                target,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state != DeleteConsumerGroupsState::AwaitingDriver {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        }
        self.state = DeleteConsumerGroupsState::Submitted;
        Ok(DeleteConsumerGroupsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: super::DeleteConsumerGroupsOutcome,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state != DeleteConsumerGroupsState::Submitted {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        }
        let Some(target) = self.plan.targets().get(self.next_target) else {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        };
        if target.group_id() != outcome.group_id() || !outcome_has_bounded_diagnostic(&outcome) {
            return self.finish_failure(
                DeleteConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            );
        }
        self.outcomes.push(outcome);
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_target += 1;
        if self.next_target == self.plan.targets().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            return Ok(self.finish(DeleteConsumerGroupsTerminal::Deleted(
                DeleteConsumerGroupsBatch::new(self.maximum_throttle_time_ms, outcomes),
            )));
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: DeleteConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state != DeleteConsumerGroupsState::AwaitingDriver {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    fn finish_submitted(
        &mut self,
        kind: DeleteConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        if self.state != DeleteConsumerGroupsState::Submitted {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        }
        self.finish_failure(kind, delivery)
    }

    fn finish_failure(
        &mut self,
        kind: DeleteConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteConsumerGroupsTransition, DeleteConsumerGroupsMachineError> {
        let Some(failed_target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(DeleteConsumerGroupsMachineError::InvalidState);
        };
        let completed = core::mem::take(&mut self.outcomes);
        let unattempted = self
            .plan
            .targets()
            .get(self.next_target.saturating_add(1)..)
            .unwrap_or_default()
            .to_vec();
        Ok(self.finish(DeleteConsumerGroupsTerminal::Failed(
            DeleteConsumerGroupsFailure::new(
                kind,
                delivery,
                self.maximum_throttle_time_ms,
                completed,
                failed_target,
                unattempted,
            ),
        )))
    }

    fn finish(&mut self, terminal: DeleteConsumerGroupsTerminal) -> DeleteConsumerGroupsTransition {
        self.state = DeleteConsumerGroupsState::Completed;
        DeleteConsumerGroupsTransition::one(DeleteConsumerGroupsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn outcome_has_bounded_diagnostic(outcome: &super::DeleteConsumerGroupsOutcome) -> bool {
    let super::DeleteConsumerGroupsResult::Failed(error) = outcome.result() else {
        return true;
    };
    match error.message() {
        Some(message) => message.len() <= super::DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES,
        None => !error.message_truncated(),
    }
}
