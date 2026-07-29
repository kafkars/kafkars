//! Atomic ID iteration and terminal assignment for Admin `FenceProducers`.

use crate::DeliveryStatus;

use super::{
    AdminFenceProducerOutcome, AdminFenceProducersBatch, AdminFenceProducersEffect,
    AdminFenceProducersFailure, AdminFenceProducersFailureKind, AdminFenceProducersInput,
    AdminFenceProducersMachine, AdminFenceProducersMachineError, AdminFenceProducersState,
    AdminFenceProducersTerminal, AdminFenceProducersTransition,
};

impl AdminFenceProducersMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminFenceProducersInput,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state == AdminFenceProducersState::Completed {
            return Err(AdminFenceProducersMachineError::AlreadyCompleted);
        }
        match input {
            AdminFenceProducersInput::Start { now } => self.start(now),
            AdminFenceProducersInput::DriverAccepted => self.driver_accepted(),
            AdminFenceProducersInput::DriverRejected => self.finish_awaiting(
                AdminFenceProducersFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            AdminFenceProducersInput::DeadlineElapsed => self.finish_awaiting(
                AdminFenceProducersFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            AdminFenceProducersInput::DriverDeadlineElapsed { delivery } => self.finish_submitted(
                AdminFenceProducersFailureKind::DeadlineElapsed,
                self.aggregate_delivery(delivery),
            ),
            AdminFenceProducersInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminFenceProducersInput::ResponseTooLarge => self.finish_submitted(
                AdminFenceProducersFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminFenceProducersInput::ProtocolIncompatible { delivery } => self.finish_submitted(
                AdminFenceProducersFailureKind::Compatibility,
                self.aggregate_delivery(delivery),
            ),
            AdminFenceProducersInput::TransportFailed { delivery } => self.finish_submitted(
                AdminFenceProducersFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminFenceProducersInput::InvalidResponse => self.finish_submitted(
                AdminFenceProducersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state != AdminFenceProducersState::Ready {
            return Err(AdminFenceProducersMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminFenceProducersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        let Some(transactional_id) = self
            .plan
            .transactional_ids()
            .get(self.next_transaction)
            .cloned()
        else {
            return Err(AdminFenceProducersMachineError::InvalidState);
        };
        self.state = AdminFenceProducersState::AwaitingDriver;
        Ok(AdminFenceProducersTransition::one(
            AdminFenceProducersEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                transactional_id,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state != AdminFenceProducersState::AwaitingDriver {
            return Err(AdminFenceProducersMachineError::InvalidState);
        }
        self.state = AdminFenceProducersState::Submitted;
        Ok(AdminFenceProducersTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: AdminFenceProducerOutcome,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state != AdminFenceProducersState::Submitted {
            return Err(AdminFenceProducersMachineError::InvalidState);
        }
        let Some(transactional_id) = self.plan.transactional_ids().get(self.next_transaction)
        else {
            return Err(AdminFenceProducersMachineError::InvalidState);
        };
        if transactional_id != outcome.transactional_id() {
            return Ok(self.invalid_response());
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.outcomes.push(outcome);
        self.next_transaction += 1;
        if self.next_transaction == self.plan.transactional_ids().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            let batch = AdminFenceProducersBatch::new(self.maximum_throttle_time_ms, outcomes);
            return Ok(self.finish(AdminFenceProducersTerminal::Fenced(batch)));
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: AdminFenceProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state != AdminFenceProducersState::AwaitingDriver {
            return Err(AdminFenceProducersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AdminFenceProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminFenceProducersTransition, AdminFenceProducersMachineError> {
        if self.state != AdminFenceProducersState::Submitted {
            return Err(AdminFenceProducersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_transaction == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_transaction == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn invalid_response(&mut self) -> AdminFenceProducersTransition {
        self.finish_failure(
            AdminFenceProducersFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        )
    }

    fn finish_failure(
        &mut self,
        kind: AdminFenceProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminFenceProducersTransition {
        self.finish(AdminFenceProducersTerminal::Failed(
            AdminFenceProducersFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: AdminFenceProducersTerminal) -> AdminFenceProducersTransition {
        self.state = AdminFenceProducersState::Completed;
        AdminFenceProducersTransition::one(AdminFenceProducersEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
