//! Atomic target iteration and terminal assignment for Admin `DescribeProducers`.

use crate::DeliveryStatus;

use super::{
    AdminDescribeProducerOutcome, AdminDescribeProducerResult, AdminDescribeProducersBatch,
    AdminDescribeProducersEffect, AdminDescribeProducersFailure, AdminDescribeProducersFailureKind,
    AdminDescribeProducersInput, AdminDescribeProducersMachine, AdminDescribeProducersMachineError,
    AdminDescribeProducersState, AdminDescribeProducersTerminal, AdminDescribeProducersTransition,
    DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES, DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES,
};

impl AdminDescribeProducersMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminDescribeProducersInput,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state == AdminDescribeProducersState::Completed {
            return Err(AdminDescribeProducersMachineError::AlreadyCompleted);
        }
        match input {
            AdminDescribeProducersInput::Start { now } => self.start(now),
            AdminDescribeProducersInput::DriverAccepted => self.driver_accepted(),
            AdminDescribeProducersInput::DriverRejected => self.finish_awaiting(
                AdminDescribeProducersFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            AdminDescribeProducersInput::DeadlineElapsed => self.finish_awaiting(
                AdminDescribeProducersFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            AdminDescribeProducersInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AdminDescribeProducersFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeProducersInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminDescribeProducersInput::ResponseTooLarge => self.finish_submitted(
                AdminDescribeProducersFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminDescribeProducersInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AdminDescribeProducersFailureKind::Compatibility,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeProducersInput::TransportFailed { delivery } => self.finish_submitted(
                AdminDescribeProducersFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminDescribeProducersInput::InvalidResponse => self.finish_submitted(
                AdminDescribeProducersFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state != AdminDescribeProducersState::Ready {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminDescribeProducersFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        let Some(target) = self.plan.targets().get(self.next_target).cloned() else {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        };
        self.state = AdminDescribeProducersState::AwaitingDriver;
        Ok(AdminDescribeProducersTransition::one(
            AdminDescribeProducersEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                target,
                broker_id: self.plan.broker_id(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state != AdminDescribeProducersState::AwaitingDriver {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        }
        self.state = AdminDescribeProducersState::Submitted;
        Ok(AdminDescribeProducersTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        mut outcome: AdminDescribeProducerOutcome,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state != AdminDescribeProducersState::Submitted {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        }
        let Some(target) = self.plan.targets().get(self.next_target) else {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        };
        if target.topic() != outcome.topic() || target.partition() != outcome.partition() {
            return Ok(self.invalid_response());
        }
        let Some(producer_state_count) = normalize_outcome(&mut outcome, self.producer_state_count)
        else {
            return Ok(self.invalid_response());
        };
        self.producer_state_count = producer_state_count;
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.outcomes.push(outcome);
        self.next_target += 1;
        if self.next_target == self.plan.targets().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            let batch = AdminDescribeProducersBatch::new(self.maximum_throttle_time_ms, outcomes);
            return Ok(self.finish(AdminDescribeProducersTerminal::Described(batch)));
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: AdminDescribeProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state != AdminDescribeProducersState::AwaitingDriver {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AdminDescribeProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeProducersTransition, AdminDescribeProducersMachineError> {
        if self.state != AdminDescribeProducersState::Submitted {
            return Err(AdminDescribeProducersMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_target == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_target == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn invalid_response(&mut self) -> AdminDescribeProducersTransition {
        self.finish_failure(
            AdminDescribeProducersFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        )
    }

    fn finish_failure(
        &mut self,
        kind: AdminDescribeProducersFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminDescribeProducersTransition {
        self.finish(AdminDescribeProducersTerminal::Failed(
            AdminDescribeProducersFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AdminDescribeProducersTerminal,
    ) -> AdminDescribeProducersTransition {
        self.state = AdminDescribeProducersState::Completed;
        AdminDescribeProducersTransition::one(AdminDescribeProducersEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn normalize_outcome(
    outcome: &mut AdminDescribeProducerOutcome,
    existing_count: usize,
) -> Option<usize> {
    if let Some(producers) = outcome.producers_mut() {
        let producer_count = existing_count.checked_add(producers.len())?;
        if producer_count > DESCRIBE_PRODUCERS_MAX_PRODUCER_STATES
            || producers.iter().any(|producer| !producer.is_well_formed())
        {
            return None;
        }
        producers.sort_unstable_by_key(|producer| producer.producer_id());
        if producers
            .windows(2)
            .any(|pair| pair[0].producer_id() == pair[1].producer_id())
        {
            return None;
        }
        return Some(producer_count);
    }
    match outcome.result() {
        AdminDescribeProducerResult::BrokerFailed(error)
            if error
                .message()
                .is_none_or(|message| message.len() <= DESCRIBE_PRODUCERS_DIAGNOSTIC_BYTES) =>
        {
            Some(existing_count)
        }
        AdminDescribeProducerResult::BrokerFailed(_)
        | AdminDescribeProducerResult::Described(_) => None,
    }
}
