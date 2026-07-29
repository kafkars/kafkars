//! Atomic singular and batched group-offset transitions and terminal assignment.

mod batch;
mod validation;

#[cfg(test)]
mod batch_test;

use crate::DeliveryStatus;

use super::{
    ListConsumerGroupOffsetsEffect, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsMachineError,
    ListConsumerGroupOffsetsState, ListConsumerGroupOffsetsTerminal,
    ListConsumerGroupOffsetsTransition,
};

impl ListConsumerGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: ListConsumerGroupOffsetsInput,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state == ListConsumerGroupOffsetsState::Completed {
            return Err(ListConsumerGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            ListConsumerGroupOffsetsInput::Start { now } => self.start(now),
            ListConsumerGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            ListConsumerGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                ListConsumerGroupOffsetsFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            ListConsumerGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            ListConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            ListConsumerGroupOffsetsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            ListConsumerGroupOffsetsInput::BrokerRejected {
                code,
                throttle_time_ms,
            } => self.broker_rejected(code, throttle_time_ms),
            ListConsumerGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                ListConsumerGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListConsumerGroupOffsetsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    ListConsumerGroupOffsetsFailureKind::Compatibility,
                    self.aggregate_delivery(delivery),
                ),
            ListConsumerGroupOffsetsInput::TransportFailed { delivery } => self.finish_submitted(
                ListConsumerGroupOffsetsFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            ListConsumerGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                ListConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::Ready {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(ListConsumerGroupOffsetsTerminal::Failed(
                ListConsumerGroupOffsetsFailure::new(
                    ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        };
        self.state = ListConsumerGroupOffsetsState::AwaitingDriver;
        Ok(ListConsumerGroupOffsetsTransition::one(
            ListConsumerGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::AwaitingDriver {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        self.state = ListConsumerGroupOffsetsState::Submitted;
        Ok(ListConsumerGroupOffsetsTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: ListConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::AwaitingDriver {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_group == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_group == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn finish_submitted(
        &mut self,
        kind: ListConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::Submitted {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: ListConsumerGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> ListConsumerGroupOffsetsTransition {
        self.finish(ListConsumerGroupOffsetsTerminal::Failed(
            ListConsumerGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: ListConsumerGroupOffsetsTerminal,
    ) -> ListConsumerGroupOffsetsTransition {
        self.state = ListConsumerGroupOffsetsState::Completed;
        ListConsumerGroupOffsetsTransition::one(ListConsumerGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
