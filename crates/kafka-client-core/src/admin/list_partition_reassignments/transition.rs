//! Atomic reassignment-listing transitions and terminal assignment.

use core::cmp::Ordering;

use crate::DeliveryStatus;

use super::{
    LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES, ListPartitionReassignmentsBatch,
    ListPartitionReassignmentsEffect, ListPartitionReassignmentsFailure,
    ListPartitionReassignmentsFailureKind, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsMachine, ListPartitionReassignmentsMachineError,
    ListPartitionReassignmentsSelection, ListPartitionReassignmentsState,
    ListPartitionReassignmentsTerminal, ListPartitionReassignmentsTransition,
    PartitionReassignmentOutcome,
};

impl ListPartitionReassignmentsMachine {
    /// Applies one normalized fact without hidden I/O, retry, cache, or cancellation.
    pub fn apply(
        &mut self,
        input: ListPartitionReassignmentsInput,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state == ListPartitionReassignmentsState::Completed {
            return Err(ListPartitionReassignmentsMachineError::AlreadyCompleted);
        }
        match input {
            ListPartitionReassignmentsInput::Start { now } => self.start(now),
            ListPartitionReassignmentsInput::DriverAccepted => self.driver_accepted(),
            ListPartitionReassignmentsInput::DriverRejected => self.finish_awaiting(
                ListPartitionReassignmentsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            ListPartitionReassignmentsInput::DeadlineElapsed => self.finish_awaiting(
                ListPartitionReassignmentsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ListPartitionReassignmentsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    ListPartitionReassignmentsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            ListPartitionReassignmentsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            ListPartitionReassignmentsInput::BrokerRejected { error } => {
                if error.message().is_some_and(|message| {
                    message.len() > LIST_PARTITION_REASSIGNMENTS_DIAGNOSTIC_BYTES
                }) {
                    return self.finish_submitted(
                        ListPartitionReassignmentsFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    );
                }
                self.finish_submitted(
                    ListPartitionReassignmentsFailureKind::Broker(error),
                    DeliveryStatus::PossiblySent,
                )
            }
            ListPartitionReassignmentsInput::ResponseTooLarge => self.finish_submitted(
                ListPartitionReassignmentsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListPartitionReassignmentsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    ListPartitionReassignmentsFailureKind::Compatibility,
                    delivery,
                ),
            ListPartitionReassignmentsInput::TransportFailed { delivery } => {
                self.finish_submitted(ListPartitionReassignmentsFailureKind::Transport, delivery)
            }
            ListPartitionReassignmentsInput::InvalidResponse => self.finish_submitted(
                ListPartitionReassignmentsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state != ListPartitionReassignmentsState::Ready {
            return Err(ListPartitionReassignmentsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(ListPartitionReassignmentsTerminal::Failed(
                ListPartitionReassignmentsFailure::new(
                    ListPartitionReassignmentsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.state = ListPartitionReassignmentsState::AwaitingDriver;
        Ok(ListPartitionReassignmentsTransition::one(
            ListPartitionReassignmentsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state != ListPartitionReassignmentsState::AwaitingDriver {
            return Err(ListPartitionReassignmentsMachineError::InvalidState);
        }
        self.state = ListPartitionReassignmentsState::Submitted;
        Ok(ListPartitionReassignmentsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: ListPartitionReassignmentsBatch,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state != ListPartitionReassignmentsState::Submitted {
            return Err(ListPartitionReassignmentsMachineError::InvalidState);
        }
        if !outcomes_match_selection(self.plan.selection(), batch.reassignments()) {
            return Ok(self.finish(ListPartitionReassignmentsTerminal::Failed(
                ListPartitionReassignmentsFailure::new(
                    ListPartitionReassignmentsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        Ok(self.finish(ListPartitionReassignmentsTerminal::Reassignments(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: ListPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state != ListPartitionReassignmentsState::AwaitingDriver {
            return Err(ListPartitionReassignmentsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: ListPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListPartitionReassignmentsTransition, ListPartitionReassignmentsMachineError> {
        if self.state != ListPartitionReassignmentsState::Submitted {
            return Err(ListPartitionReassignmentsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: ListPartitionReassignmentsFailureKind,
        delivery: DeliveryStatus,
    ) -> ListPartitionReassignmentsTransition {
        self.finish(ListPartitionReassignmentsTerminal::Failed(
            ListPartitionReassignmentsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: ListPartitionReassignmentsTerminal,
    ) -> ListPartitionReassignmentsTransition {
        self.state = ListPartitionReassignmentsState::Completed;
        ListPartitionReassignmentsTransition::one(ListPartitionReassignmentsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn outcomes_match_selection(
    selection: &ListPartitionReassignmentsSelection,
    outcomes: &[PartitionReassignmentOutcome],
) -> bool {
    if outcomes.iter().any(outcome_is_malformed) {
        return false;
    }
    match selection {
        ListPartitionReassignmentsSelection::Selected(targets) => {
            let mut cursor = 0usize;
            for outcome in outcomes {
                while cursor < targets.len()
                    && (targets[cursor].topic() != outcome.topic()
                        || targets[cursor].partition() != outcome.partition())
                {
                    cursor += 1;
                }
                if cursor == targets.len() {
                    return false;
                }
                cursor += 1;
            }
            true
        }
        ListPartitionReassignmentsSelection::AllActive => {
            outcomes.windows(2).all(|pair| {
                match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
                    Ordering::Less => true,
                    Ordering::Equal => pair[0].partition() < pair[1].partition(),
                    Ordering::Greater => false,
                }
            })
        }
    }
}

fn outcome_is_malformed(outcome: &PartitionReassignmentOutcome) -> bool {
    if outcome.topic().is_empty()
        || outcome.partition() < 0
        || outcome.reassignment().replicas().is_empty()
    {
        return true;
    }
    let lists = [
        outcome.reassignment().replicas(),
        outcome.reassignment().adding_replicas(),
        outcome.reassignment().removing_replicas(),
    ];
    if lists.iter().any(|brokers| {
        brokers.iter().enumerate().any(|(index, broker)| {
            *broker < 0 || brokers[..index].iter().any(|earlier| earlier == broker)
        })
    }) {
        return true;
    }
    outcome
        .reassignment()
        .adding_replicas()
        .iter()
        .any(|broker| outcome.reassignment().removing_replicas().contains(broker))
}
