//! Atomic admission, correlation, and terminal assignment for one group commit.

use crate::{Deadline, DeliveryStatus, OperationId};

use super::{
    GroupAssignmentPartition, GroupCheckpoint, GroupOffsetCommitAdmission,
    GroupOffsetCommitAdmissionError as AdmissionError,
    GroupOffsetCommitAdmissionErrorKind as AdmissionErrorKind, GroupOffsetCommitApplyError,
    GroupOffsetCommitBatch, GroupOffsetCommitEffect, GroupOffsetCommitFailure,
    GroupOffsetCommitFailureKind, GroupOffsetCommitInput, GroupOffsetCommitMachineError,
    GroupOffsetCommitPartitionOutcome, GroupOffsetCommitState, GroupOffsetCommitTerminal,
    GroupOffsetCommitTransition, LiveGroupAssignment, assignment::reserve_expected_partitions,
    validate_group_offset_commit_checkpoint,
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
        if let Err(kind) = validate_group_offset_commit_checkpoint(live_assignment, &checkpoint) {
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
    ) -> Result<GroupOffsetCommitTransition, GroupOffsetCommitApplyError> {
        if let Err(kind) = self.validate(&input) {
            return Err(GroupOffsetCommitApplyError::new(kind, input));
        }

        let transition = match input {
            GroupOffsetCommitInput::DriverAccepted => {
                self.state = GroupOffsetCommitState::Submitted;
                GroupOffsetCommitTransition::none()
            }
            GroupOffsetCommitInput::DriverRejected => self.finish_failure(
                GroupOffsetCommitFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            GroupOffsetCommitInput::ExecutionUnavailable => self.finish_failure(
                GroupOffsetCommitFailureKind::ExecutionUnavailable,
                DeliveryStatus::NotSent,
            ),
            GroupOffsetCommitInput::DeadlineElapsed { delivery } => {
                self.finish_failure(GroupOffsetCommitFailureKind::DeadlineElapsed, delivery)
            }
            GroupOffsetCommitInput::BrokerResponded {
                throttle_time_ms,
                outcomes,
            } => self.broker_responded(throttle_time_ms, outcomes),
            GroupOffsetCommitInput::ProtocolIncompatible { delivery } => {
                self.finish_failure(GroupOffsetCommitFailureKind::Compatibility, delivery)
            }
            GroupOffsetCommitInput::ResponseTooLarge => self.finish_failure(
                GroupOffsetCommitFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            GroupOffsetCommitInput::InvalidResponse => self.finish_failure(
                GroupOffsetCommitFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
            GroupOffsetCommitInput::TransportFailed { delivery } => {
                self.finish_failure(GroupOffsetCommitFailureKind::Transport, delivery)
            }
        };
        Ok(transition)
    }

    fn validate(
        &self,
        input: &GroupOffsetCommitInput,
    ) -> Result<(), GroupOffsetCommitMachineError> {
        if self.state == GroupOffsetCommitState::Completed {
            return Err(GroupOffsetCommitMachineError::AlreadyCompleted);
        }

        match (self.state, input) {
            (
                GroupOffsetCommitState::AwaitingDriver,
                GroupOffsetCommitInput::DeadlineElapsed {
                    delivery: DeliveryStatus::PossiblySent,
                },
            ) => Err(GroupOffsetCommitMachineError::InvalidDeliveryStatus),
            (
                GroupOffsetCommitState::AwaitingDriver,
                GroupOffsetCommitInput::DriverAccepted
                | GroupOffsetCommitInput::DriverRejected
                | GroupOffsetCommitInput::ExecutionUnavailable
                | GroupOffsetCommitInput::DeadlineElapsed {
                    delivery: DeliveryStatus::NotSent,
                },
            )
            | (
                GroupOffsetCommitState::Submitted,
                GroupOffsetCommitInput::DeadlineElapsed { .. }
                | GroupOffsetCommitInput::BrokerResponded { .. }
                | GroupOffsetCommitInput::ProtocolIncompatible { .. }
                | GroupOffsetCommitInput::ResponseTooLarge
                | GroupOffsetCommitInput::InvalidResponse
                | GroupOffsetCommitInput::TransportFailed { .. },
            ) => Ok(()),
            (GroupOffsetCommitState::AwaitingDriver | GroupOffsetCommitState::Submitted, _) => {
                Err(GroupOffsetCommitMachineError::InvalidState)
            }
            (GroupOffsetCommitState::Completed, _) => {
                Err(GroupOffsetCommitMachineError::AlreadyCompleted)
            }
        }
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
    ) -> GroupOffsetCommitTransition {
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
            return self.finish(GroupOffsetCommitTerminal::Failed(
                GroupOffsetCommitFailure::new(
                    GroupOffsetCommitFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            ));
        }
        let terminal = GroupOffsetCommitBatch::new(throttle_time_ms, outcomes).into_terminal();
        self.finish(terminal)
    }

    fn finish_failure(
        &mut self,
        kind: GroupOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    ) -> GroupOffsetCommitTransition {
        self.finish(GroupOffsetCommitTerminal::Failed(
            GroupOffsetCommitFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: GroupOffsetCommitTerminal) -> GroupOffsetCommitTransition {
        self.state = GroupOffsetCommitState::Completed;
        GroupOffsetCommitTransition::one(GroupOffsetCommitEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
