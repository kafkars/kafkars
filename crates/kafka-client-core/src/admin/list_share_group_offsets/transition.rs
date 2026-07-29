//! Atomic caller-ordered API-90 lifecycle and terminal assignment.

mod batch;

use crate::DeliveryStatus;

use super::{
    ListShareGroupOffsetsEffect, ListShareGroupOffsetsFailure, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsInput, ListShareGroupOffsetsMachine, ListShareGroupOffsetsMachineError,
    ListShareGroupOffsetsState, ListShareGroupOffsetsTerminal, ListShareGroupOffsetsTransition,
};

impl ListShareGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: ListShareGroupOffsetsInput,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state == ListShareGroupOffsetsState::Completed {
            return Err(ListShareGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            ListShareGroupOffsetsInput::Start { now } => self.start(now),
            ListShareGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            ListShareGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                ListShareGroupOffsetsFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            ListShareGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                ListShareGroupOffsetsFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            ListShareGroupOffsetsInput::DriverDeadlineElapsed { delivery } => {
                let delivery = self.aggregate_delivery(delivery);
                self.finish_submitted(ListShareGroupOffsetsFailureKind::DeadlineElapsed, delivery)
            }
            ListShareGroupOffsetsInput::BrokerResponded { batch } => self.broker_responded(batch),
            ListShareGroupOffsetsInput::BrokerRejected { error } => self.broker_rejected(error),
            ListShareGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListShareGroupOffsetsInput::ProtocolIncompatible { delivery } => {
                let delivery = self.aggregate_delivery(delivery);
                self.finish_submitted(ListShareGroupOffsetsFailureKind::Compatibility, delivery)
            }
            ListShareGroupOffsetsInput::TransportFailed { delivery } => {
                let delivery = self.aggregate_delivery(delivery);
                self.finish_submitted(ListShareGroupOffsetsFailureKind::Transport, delivery)
            }
            ListShareGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                ListShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::Ready {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                ListShareGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    pub(super) fn submit_current(
        &mut self,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        };
        self.state = ListShareGroupOffsetsState::AwaitingDriver;
        Ok(ListShareGroupOffsetsTransition::one(
            ListShareGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::AwaitingDriver {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        }
        self.state = ListShareGroupOffsetsState::Submitted;
        Ok(ListShareGroupOffsetsTransition::none())
    }

    fn finish_awaiting(
        &mut self,
        kind: ListShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::AwaitingDriver {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
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
        kind: ListShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::Submitted {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_failure(
        &mut self,
        kind: ListShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> ListShareGroupOffsetsTransition {
        self.finish(ListShareGroupOffsetsTerminal::Failed(
            ListShareGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    pub(super) fn finish(
        &mut self,
        terminal: ListShareGroupOffsetsTerminal,
    ) -> ListShareGroupOffsetsTransition {
        self.state = ListShareGroupOffsetsState::Completed;
        ListShareGroupOffsetsTransition::one(ListShareGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
