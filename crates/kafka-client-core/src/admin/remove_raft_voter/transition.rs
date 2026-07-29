//! Destructive voter-removal transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    REMOVE_RAFT_VOTER_DIAGNOSTIC_BYTES, RemoveRaftVoterEffect, RemoveRaftVoterFailure,
    RemoveRaftVoterFailureKind, RemoveRaftVoterInput, RemoveRaftVoterMachine,
    RemoveRaftVoterMachineError, RemoveRaftVoterState, RemoveRaftVoterTerminal,
    RemoveRaftVoterTransition,
};

impl RemoveRaftVoterMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: RemoveRaftVoterInput,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state == RemoveRaftVoterState::Completed {
            return Err(RemoveRaftVoterMachineError::AlreadyCompleted);
        }
        match input {
            RemoveRaftVoterInput::Start { now } => self.start(now),
            RemoveRaftVoterInput::DriverAccepted => self.driver_accepted(),
            RemoveRaftVoterInput::DriverRejected => self.finish_awaiting(
                RemoveRaftVoterFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            RemoveRaftVoterInput::DeadlineElapsed => self.finish_awaiting(
                RemoveRaftVoterFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            RemoveRaftVoterInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(RemoveRaftVoterFailureKind::DeadlineElapsed, delivery)
            }
            RemoveRaftVoterInput::BrokerResponded { success } => {
                self.finish_submitted_terminal(RemoveRaftVoterTerminal::Removed(success))
            }
            RemoveRaftVoterInput::BrokerRejected { error } => {
                if diagnostic_is_invalid(&error) {
                    self.finish_submitted(
                        RemoveRaftVoterFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    )
                } else {
                    self.finish_submitted_terminal(RemoveRaftVoterTerminal::BrokerRejected(error))
                }
            }
            RemoveRaftVoterInput::ResponseTooLarge => self.finish_submitted(
                RemoveRaftVoterFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            RemoveRaftVoterInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(RemoveRaftVoterFailureKind::Compatibility, delivery)
            }
            RemoveRaftVoterInput::TransportFailed { delivery } => {
                self.finish_submitted(RemoveRaftVoterFailureKind::Transport, delivery)
            }
            RemoveRaftVoterInput::InvalidResponse => self.finish_submitted(
                RemoveRaftVoterFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state != RemoveRaftVoterState::Ready {
            return Err(RemoveRaftVoterMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                RemoveRaftVoterFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = RemoveRaftVoterState::AwaitingDriver;
        Ok(RemoveRaftVoterTransition::one(
            RemoveRaftVoterEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state != RemoveRaftVoterState::AwaitingDriver {
            return Err(RemoveRaftVoterMachineError::InvalidState);
        }
        self.state = RemoveRaftVoterState::Submitted;
        Ok(RemoveRaftVoterTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: RemoveRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state != RemoveRaftVoterState::AwaitingDriver {
            return Err(RemoveRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: RemoveRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state != RemoveRaftVoterState::Submitted {
            return Err(RemoveRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: RemoveRaftVoterTerminal,
    ) -> Result<RemoveRaftVoterTransition, RemoveRaftVoterMachineError> {
        if self.state != RemoveRaftVoterState::Submitted {
            return Err(RemoveRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: RemoveRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> RemoveRaftVoterTransition {
        self.finish(RemoveRaftVoterTerminal::Failed(
            RemoveRaftVoterFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: RemoveRaftVoterTerminal) -> RemoveRaftVoterTransition {
        self.state = RemoveRaftVoterState::Completed;
        RemoveRaftVoterTransition::one(RemoveRaftVoterEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn diagnostic_is_invalid(error: &super::RemoveRaftVoterBrokerError) -> bool {
    error
        .message()
        .is_some_and(|message| message.len() > REMOVE_RAFT_VOTER_DIAGNOSTIC_BYTES)
        || (error.message().is_none() && error.message_truncated())
}
