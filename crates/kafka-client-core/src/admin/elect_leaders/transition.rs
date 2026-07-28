//! Atomic election transitions and single terminal assignment.

use crate::DeliveryStatus;

use super::{
    ElectLeadersBatch, ElectLeadersEffect, ElectLeadersFailure, ElectLeadersFailureKind,
    ElectLeadersInput, ElectLeadersMachine, ElectLeadersMachineError, ElectLeadersState,
    ElectLeadersTerminal, ElectLeadersTransition, LeaderElectionOutcome,
};

impl ElectLeadersMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: ElectLeadersInput,
    ) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state == ElectLeadersState::Completed {
            return Err(ElectLeadersMachineError::AlreadyCompleted);
        }
        match input {
            ElectLeadersInput::Start { now } => self.start(now),
            ElectLeadersInput::DriverAccepted => self.driver_accepted(),
            ElectLeadersInput::DriverRejected => self.finish_awaiting(
                ElectLeadersFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            ElectLeadersInput::DeadlineElapsed => self.finish_awaiting(
                ElectLeadersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ElectLeadersInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(ElectLeadersFailureKind::DeadlineElapsed, delivery)
            }
            ElectLeadersInput::BrokerResponded { batch } => self.broker_responded(batch),
            ElectLeadersInput::BrokerRejected { error } => self.finish_submitted(
                ElectLeadersFailureKind::Broker(error),
                DeliveryStatus::PossiblySent,
            ),
            ElectLeadersInput::ResponseTooLarge => self.finish_submitted(
                ElectLeadersFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ElectLeadersInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(ElectLeadersFailureKind::Compatibility, delivery)
            }
            ElectLeadersInput::TransportFailed { delivery } => {
                self.finish_submitted(ElectLeadersFailureKind::Transport, delivery)
            }
            ElectLeadersInput::InvalidResponse => self.finish_submitted(
                ElectLeadersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state != ElectLeadersState::Ready {
            return Err(ElectLeadersMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                ElectLeadersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = ElectLeadersState::AwaitingDriver;
        Ok(ElectLeadersTransition::one(ElectLeadersEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan: self.plan.clone(),
        }))
    }

    fn driver_accepted(&mut self) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state != ElectLeadersState::AwaitingDriver {
            return Err(ElectLeadersMachineError::InvalidState);
        }
        self.state = ElectLeadersState::Submitted;
        Ok(ElectLeadersTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: ElectLeadersBatch,
    ) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state != ElectLeadersState::Submitted {
            return Err(ElectLeadersMachineError::InvalidState);
        }
        if !self.outcomes_match_plan(batch.outcomes()) {
            return Ok(self.finish_failure(
                ElectLeadersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(ElectLeadersTerminal::Elected(batch)))
    }

    fn outcomes_match_plan(&self, outcomes: &[LeaderElectionOutcome]) -> bool {
        self.plan.targets().len() == outcomes.len()
            && self
                .plan
                .targets()
                .iter()
                .zip(outcomes)
                .all(|(change, outcome)| {
                    change.topic() == outcome.topic() && change.partition() == outcome.partition()
                })
    }

    fn finish_awaiting(
        &mut self,
        kind: ElectLeadersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state != ElectLeadersState::AwaitingDriver {
            return Err(ElectLeadersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: ElectLeadersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ElectLeadersTransition, ElectLeadersMachineError> {
        if self.state != ElectLeadersState::Submitted {
            return Err(ElectLeadersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: ElectLeadersFailureKind,
        delivery: DeliveryStatus,
    ) -> ElectLeadersTransition {
        self.finish(ElectLeadersTerminal::Failed(ElectLeadersFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: ElectLeadersTerminal) -> ElectLeadersTransition {
        self.state = ElectLeadersState::Completed;
        ElectLeadersTransition::one(ElectLeadersEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
