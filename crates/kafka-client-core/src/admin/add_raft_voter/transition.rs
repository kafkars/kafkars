//! Committed voter-addition transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    ADD_RAFT_VOTER_DIAGNOSTIC_BYTES, AddRaftVoterEffect, AddRaftVoterFailure,
    AddRaftVoterFailureKind, AddRaftVoterInput, AddRaftVoterMachine, AddRaftVoterMachineError,
    AddRaftVoterState, AddRaftVoterTerminal, AddRaftVoterTransition,
};

impl AddRaftVoterMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AddRaftVoterInput,
    ) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state == AddRaftVoterState::Completed {
            return Err(AddRaftVoterMachineError::AlreadyCompleted);
        }
        match input {
            AddRaftVoterInput::Start { now } => self.start(now),
            AddRaftVoterInput::DriverAccepted => self.driver_accepted(),
            AddRaftVoterInput::DriverRejected => self.finish_awaiting(
                AddRaftVoterFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AddRaftVoterInput::DeadlineElapsed => self.finish_awaiting(
                AddRaftVoterFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AddRaftVoterInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(AddRaftVoterFailureKind::DeadlineElapsed, delivery)
            }
            AddRaftVoterInput::BrokerResponded { success } => {
                self.finish_submitted_terminal(AddRaftVoterTerminal::Added(success))
            }
            AddRaftVoterInput::BrokerRejected { error } => {
                if diagnostic_is_invalid(&error) {
                    self.finish_submitted(
                        AddRaftVoterFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    )
                } else {
                    self.finish_submitted_terminal(AddRaftVoterTerminal::BrokerRejected(error))
                }
            }
            AddRaftVoterInput::ResponseTooLarge => self.finish_submitted(
                AddRaftVoterFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AddRaftVoterInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(AddRaftVoterFailureKind::Compatibility, delivery)
            }
            AddRaftVoterInput::TransportFailed { delivery } => {
                self.finish_submitted(AddRaftVoterFailureKind::Transport, delivery)
            }
            AddRaftVoterInput::InvalidResponse => self.finish_submitted(
                AddRaftVoterFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state != AddRaftVoterState::Ready {
            return Err(AddRaftVoterMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AddRaftVoterFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AddRaftVoterState::AwaitingDriver;
        Ok(AddRaftVoterTransition::one(AddRaftVoterEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan: self.plan.clone(),
        }))
    }

    fn driver_accepted(&mut self) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state != AddRaftVoterState::AwaitingDriver {
            return Err(AddRaftVoterMachineError::InvalidState);
        }
        self.state = AddRaftVoterState::Submitted;
        Ok(AddRaftVoterTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: AddRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state != AddRaftVoterState::AwaitingDriver {
            return Err(AddRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AddRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state != AddRaftVoterState::Submitted {
            return Err(AddRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: AddRaftVoterTerminal,
    ) -> Result<AddRaftVoterTransition, AddRaftVoterMachineError> {
        if self.state != AddRaftVoterState::Submitted {
            return Err(AddRaftVoterMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: AddRaftVoterFailureKind,
        delivery: DeliveryStatus,
    ) -> AddRaftVoterTransition {
        self.finish(AddRaftVoterTerminal::Failed(AddRaftVoterFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: AddRaftVoterTerminal) -> AddRaftVoterTransition {
        self.state = AddRaftVoterState::Completed;
        AddRaftVoterTransition::one(AddRaftVoterEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn diagnostic_is_invalid(error: &super::AddRaftVoterBrokerError) -> bool {
    error
        .message()
        .is_some_and(|message| message.len() > ADD_RAFT_VOTER_DIAGNOSTIC_BYTES)
        || (error.message().is_none() && error.message_truncated())
}
