//! Atomic group-offset lifecycle transitions and terminal assignment.

use core::cmp::Ordering;

use crate::DeliveryStatus;

use super::{
    GroupOffsetOutcome, GroupOffsetResult, ListConsumerGroupOffsetsBatch,
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
                DeliveryStatus::NotSent,
            ),
            ListConsumerGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ListConsumerGroupOffsetsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            ListConsumerGroupOffsetsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            ListConsumerGroupOffsetsInput::BrokerRejected { code } => self.finish_submitted(
                ListConsumerGroupOffsetsFailureKind::Broker(code),
                DeliveryStatus::PossiblySent,
            ),
            ListConsumerGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                ListConsumerGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListConsumerGroupOffsetsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(ListConsumerGroupOffsetsFailureKind::Compatibility, delivery)
            }
            ListConsumerGroupOffsetsInput::TransportFailed { delivery } => {
                self.finish_submitted(ListConsumerGroupOffsetsFailureKind::Transport, delivery)
            }
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
        self.state = ListConsumerGroupOffsetsState::AwaitingDriver;
        Ok(ListConsumerGroupOffsetsTransition::one(
            ListConsumerGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
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

    fn broker_responded(
        &mut self,
        batch: ListConsumerGroupOffsetsBatch,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::Submitted {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        if !outcomes_are_normalized(batch.outcomes()) {
            return Ok(self.finish(ListConsumerGroupOffsetsTerminal::Failed(
                ListConsumerGroupOffsetsFailure::new(
                    ListConsumerGroupOffsetsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        Ok(self.finish(ListConsumerGroupOffsetsTerminal::Offsets(batch)))
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

fn outcomes_are_normalized(outcomes: &[GroupOffsetOutcome]) -> bool {
    if outcomes.iter().any(outcome_is_malformed) {
        return false;
    }
    outcomes.windows(2).all(|pair| {
        match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
            Ordering::Less => true,
            Ordering::Equal => pair[0].partition() < pair[1].partition(),
            Ordering::Greater => false,
        }
    })
}

fn outcome_is_malformed(outcome: &GroupOffsetOutcome) -> bool {
    if outcome.topic().is_empty() || outcome.partition() < 0 {
        return true;
    }
    let GroupOffsetResult::Described(description) = outcome.result() else {
        return false;
    };
    description.offset().is_some_and(|offset| offset < 0)
        || description.leader_epoch().is_some_and(|epoch| epoch < 0)
}
