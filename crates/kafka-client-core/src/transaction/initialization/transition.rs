//! Atomic transaction-initialization transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    TransactionInitializationEffect, TransactionInitializationFailure,
    TransactionInitializationFailureKind, TransactionInitializationInput,
    TransactionInitializationMachine, TransactionInitializationMachineError,
    TransactionInitializationState, TransactionInitializationTerminal,
    TransactionInitializationTransition, TransactionalOwnerId, TransactionalProducerIdentity,
};

impl TransactionInitializationMachine {
    /// Applies one normalized fact without hidden I/O, retry, or coordinator policy.
    pub fn apply(
        &mut self,
        owner_id: TransactionalOwnerId,
        input: TransactionInitializationInput,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if owner_id != self.owner_id {
            return Err(TransactionInitializationMachineError::OwnerMismatch {
                expected: self.owner_id,
                supplied: owner_id,
            });
        }
        if self.state == TransactionInitializationState::Completed {
            return Err(TransactionInitializationMachineError::AlreadyCompleted);
        }
        match input {
            TransactionInitializationInput::Start { now } => self.start(now),
            TransactionInitializationInput::DriverAccepted => self.driver_accepted(),
            TransactionInitializationInput::DriverRejected => self.finish_awaiting(
                TransactionInitializationFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            TransactionInitializationInput::DeadlineElapsed => self.finish_awaiting(
                TransactionInitializationFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            TransactionInitializationInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    TransactionInitializationFailureKind::DeadlineElapsed,
                    delivery,
                ),
            TransactionInitializationInput::BrokerInitialized {
                producer_id,
                producer_epoch,
            } => self.broker_initialized(producer_id, producer_epoch),
            TransactionInitializationInput::BrokerRejected { failure } => self.finish_submitted(
                TransactionInitializationFailureKind::Broker(failure),
                DeliveryStatus::PossiblySent,
            ),
            TransactionInitializationInput::TransportFailed { delivery } => {
                self.finish_submitted(TransactionInitializationFailureKind::Transport, delivery)
            }
            TransactionInitializationInput::InvalidResponse => self.finish_submitted(
                TransactionInitializationFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if self.state != TransactionInitializationState::Ready {
            return Err(TransactionInitializationMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                TransactionInitializationFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = TransactionInitializationState::AwaitingDriver;
        Ok(TransactionInitializationTransition::one(
            TransactionInitializationEffect::Submit {
                owner_id: self.owner_id,
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if self.state != TransactionInitializationState::AwaitingDriver {
            return Err(TransactionInitializationMachineError::InvalidState);
        }
        self.state = TransactionInitializationState::Submitted;
        Ok(TransactionInitializationTransition::none())
    }

    fn broker_initialized(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if self.state != TransactionInitializationState::Submitted {
            return Err(TransactionInitializationMachineError::InvalidState);
        }
        let Some(identity) = TransactionalProducerIdentity::try_new(producer_id, producer_epoch)
        else {
            return Ok(self.finish_failure(
                TransactionInitializationFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        };
        Ok(self.finish(TransactionInitializationTerminal::Initialized(identity)))
    }

    fn finish_awaiting(
        &mut self,
        kind: TransactionInitializationFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if self.state != TransactionInitializationState::AwaitingDriver {
            return Err(TransactionInitializationMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: TransactionInitializationFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<TransactionInitializationTransition, TransactionInitializationMachineError> {
        if self.state != TransactionInitializationState::Submitted {
            return Err(TransactionInitializationMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: TransactionInitializationFailureKind,
        delivery: DeliveryStatus,
    ) -> TransactionInitializationTransition {
        self.finish(TransactionInitializationTerminal::Failed(
            TransactionInitializationFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: TransactionInitializationTerminal,
    ) -> TransactionInitializationTransition {
        self.state = TransactionInitializationState::Completed;
        TransactionInitializationTransition::one(TransactionInitializationEffect::Complete {
            owner_id: self.owner_id,
            operation_id: self.operation_id,
            terminal,
        })
    }
}
