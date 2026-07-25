//! Atomic admission, correlation, and terminal assignment for one group commit.

use crate::{Deadline, DeliveryStatus, OperationId};

use super::{
    GroupAssignmentPartition, GroupCheckpoint, GroupOffsetCommitAdmission,
    GroupOffsetCommitAdmissionError as AdmissionError,
    GroupOffsetCommitAdmissionErrorKind as AdmissionErrorKind, GroupOffsetCommitBatch,
    GroupOffsetCommitEffect, GroupOffsetCommitFailure, GroupOffsetCommitFailureKind,
    GroupOffsetCommitInput, GroupOffsetCommitMachineError, GroupOffsetCommitPartitionOutcome,
    GroupOffsetCommitState, GroupOffsetCommitTerminal, GroupOffsetCommitTransition,
    LiveGroupAssignment, assignment::reserve_expected_partitions,
};

/// Deterministic owner for one capacity-reserved group offset commit.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitMachine {
    operation_id: OperationId,
    deadline: Deadline,
    expected: Vec<GroupAssignmentPartition>,
    state: GroupOffsetCommitState,
}

impl GroupOffsetCommitMachine {
    /// Validates and admits one linear checkpoint after engine completion reservation.
    pub fn try_admit(
        operation_id: OperationId,
        deadline: Deadline,
        live_assignment: Option<&LiveGroupAssignment>,
        checkpoint: GroupCheckpoint,
    ) -> Result<GroupOffsetCommitAdmission, AdmissionError> {
        let Some(assignment) = live_assignment else {
            return Err(AdmissionError::new(
                AdmissionErrorKind::AssignmentLost,
                checkpoint,
            ));
        };
        if let Err(kind) = assignment.validate_checkpoint(&checkpoint) {
            return Err(AdmissionError::new(kind, checkpoint));
        }
        let mut expected = Vec::new();
        if !reserve_expected_partitions(&mut expected, checkpoint.entries().len()) {
            return Err(AdmissionError::new(
                AdmissionErrorKind::AllocationFailed,
                checkpoint,
            ));
        }
        expected.extend(
            checkpoint
                .entries()
                .iter()
                .map(|entry| GroupAssignmentPartition::new(entry.topic_id(), entry.partition())),
        );

        let machine = Self {
            operation_id,
            deadline,
            expected,
            state: GroupOffsetCommitState::AwaitingDriver,
        };
        let submit = GroupOffsetCommitEffect::Submit {
            operation_id,
            deadline,
            checkpoint,
        };
        Ok(GroupOffsetCommitAdmission::new(machine, submit))
    }

    /// Returns the stable operation identity retained through settlement.
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns the original absolute deadline retained through settlement.
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> GroupOffsetCommitState {
        self.state
    }

    /// Returns actual retained correlation-vector capacity for engine accounting.
    pub fn expected_capacity(&self) -> usize {
        self.expected.capacity()
    }

    /// Applies one normalized fact without retry, I/O, clocks, or coordination.
    pub fn apply(
        &mut self,
        input: GroupOffsetCommitInput,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        if self.state == GroupOffsetCommitState::Completed {
            return Err(GroupOffsetCommitMachineError::AlreadyCompleted);
        }
        match input {
            GroupOffsetCommitInput::DriverAccepted => self.driver_accepted(),
            GroupOffsetCommitInput::DriverRejected => self.finish_awaiting(
                GroupOffsetCommitFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            GroupOffsetCommitInput::DeadlineElapsed { delivery } => self.deadline_elapsed(delivery),
            GroupOffsetCommitInput::BrokerResponded {
                throttle_time_ms,
                outcomes,
            } => self.broker_responded(throttle_time_ms, outcomes),
            GroupOffsetCommitInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(GroupOffsetCommitFailureKind::Compatibility, delivery)
            }
            GroupOffsetCommitInput::ResponseTooLarge => self.finish_submitted(
                GroupOffsetCommitFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            GroupOffsetCommitInput::InvalidResponse => self.finish_submitted(
                GroupOffsetCommitFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
            GroupOffsetCommitInput::TransportFailed { delivery } => {
                self.finish_submitted(GroupOffsetCommitFailureKind::Transport, delivery)
            }
        }
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        if self.state != GroupOffsetCommitState::AwaitingDriver {
            return Err(GroupOffsetCommitMachineError::InvalidState);
        }
        self.state = GroupOffsetCommitState::Submitted;
        Ok(GroupOffsetCommitTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        if self.state != GroupOffsetCommitState::Submitted {
            return Err(GroupOffsetCommitMachineError::InvalidState);
        }
        let correlated = self.expected.len() == outcomes.len()
            && self
                .expected
                .iter()
                .zip(&outcomes)
                .all(|(expected, outcome)| {
                    expected.topic_id() == outcome.topic_id()
                        && expected.partition() == outcome.partition()
                });
        if !correlated {
            return Ok(self.finish(GroupOffsetCommitTerminal::Failed(
                GroupOffsetCommitFailure::new(
                    GroupOffsetCommitFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        let terminal = GroupOffsetCommitBatch::new(throttle_time_ms, outcomes).into_terminal();
        Ok(self.finish(terminal))
    }

    fn deadline_elapsed(
        &mut self,
        delivery: DeliveryStatus,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        match self.state {
            GroupOffsetCommitState::AwaitingDriver => {
                if delivery != DeliveryStatus::NotSent {
                    return Err(GroupOffsetCommitMachineError::InvalidDeliveryStatus);
                }
                self.finish_awaiting(
                    GroupOffsetCommitFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                )
            }
            GroupOffsetCommitState::Submitted => {
                self.finish_submitted(GroupOffsetCommitFailureKind::DeadlineElapsed, delivery)
            }
            GroupOffsetCommitState::Completed => {
                Err(GroupOffsetCommitMachineError::AlreadyCompleted)
            }
        }
    }

    fn finish_awaiting(
        &mut self,
        kind: GroupOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        if self.state != GroupOffsetCommitState::AwaitingDriver {
            return Err(GroupOffsetCommitMachineError::InvalidState);
        }
        Ok(self.finish(GroupOffsetCommitTerminal::Failed(
            GroupOffsetCommitFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted(
        &mut self,
        kind: GroupOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitMachineError> {
        if self.state != GroupOffsetCommitState::Submitted {
            return Err(GroupOffsetCommitMachineError::InvalidState);
        }
        Ok(self.finish(GroupOffsetCommitTerminal::Failed(
            GroupOffsetCommitFailure::new(kind, delivery),
        )))
    }

    fn finish(&mut self, terminal: GroupOffsetCommitTerminal) -> GroupOffsetCommitTransition {
        self.state = GroupOffsetCommitState::Completed;
        GroupOffsetCommitTransition::one(GroupOffsetCommitEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
