//! Bounded ownership of partition transaction-abort machines and API27 calls.

mod admission;
mod response;
mod terminal;

use kafka_client_core::{
    AbortPartitionTransactionEffect, AbortPartitionTransactionInput,
    AbortPartitionTransactionMachine, AbortPartitionTransactionPlan,
    AbortPartitionTransactionTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAbortPartitionTransactionPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{AbortPartitionTransactionCall, AbortPartitionTransactionRawTerminal},
};

use super::{AbortPartitionTransactionHostError, AbortPartitionTransactionObserver};

pub(crate) const ABORT_PARTITION_TRANSACTION_CAPACITY: usize = 16;
pub(crate) const ABORT_PARTITION_TRANSACTION_RETAINED_BYTES: usize = 4 * 1024 * 1024;
pub(crate) struct AbortPartitionTransactionAdmission {
    pub(crate) observer: AbortPartitionTransactionObserver,
    pub(crate) fault: Option<AbortPartitionTransactionHostError>,
}

/// One validated plan ready for the later driver-admission stage.
pub(crate) struct AbortPartitionTransactionSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: AbortPartitionTransactionPlan,
}

impl AbortPartitionTransactionSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AbortPartitionTransactionPlan,
    ) {
        (self.operation_id, self.deadline, self.plan)
    }
}

pub(crate) enum AbortPartitionTransactionTurn {
    Idle,
    Progress,
    Submit(AbortPartitionTransactionSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AbortPartitionTransactionHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AbortPartitionTransactionOperation {
    operation_id: OperationId,
    machine: AbortPartitionTransactionMachine,
    plan: AbortPartitionTransactionPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    submission: Option<AbortPartitionTransactionSubmission>,
    handoff: AbortPartitionTransactionHandoff,
    call: Option<AbortPartitionTransactionCall>,
    recovered_call: Option<crate::driver::RecoveredAbortPartitionTransactionCall>,
    raw_terminal: Option<AbortPartitionTransactionRawTerminal>,
    terminal: Option<AbortPartitionTransactionTerminal>,
}

pub(crate) struct AbortPartitionTransactionHost {
    operations: Vec<AbortPartitionTransactionOperation>,
    completions: CompletionRegistry<
        AbortPartitionTransactionTerminal,
        AdminAbortPartitionTransactionPublisher,
    >,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AbortPartitionTransactionHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AbortPartitionTransactionHost {
    pub(crate) fn new(publisher: AdminAbortPartitionTransactionPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ABORT_PARTITION_TRANSACTION_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ABORT_PARTITION_TRANSACTION_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ABORT_PARTITION_TRANSACTION_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AbortPartitionTransactionTurn, AbortPartitionTransactionHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AbortPartitionTransactionTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AbortPartitionTransactionTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AbortPartitionTransactionInput::DeadlineElapsed,
            )?;
            return Ok(AbortPartitionTransactionTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AbortPartitionTransactionHostError::MissingSubmission)?;
        self.operations[index].handoff = AbortPartitionTransactionHandoff::HandedOff;
        Ok(AbortPartitionTransactionTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AbortPartitionTransactionCall,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AbortPartitionTransactionHostError::UnknownOperation)?;
        if self.operations[index].handoff != AbortPartitionTransactionHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AbortPartitionTransactionHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AbortPartitionTransactionInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AbortPartitionTransactionHostError::UnknownOperation)?;
        if self.operations[index].handoff != AbortPartitionTransactionHandoff::HandedOff {
            return Err(AbortPartitionTransactionHostError::InvalidHandoff);
        }
        self.apply(operation_id, AbortPartitionTransactionInput::DriverRejected)
    }

    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: AbortPartitionTransactionInput,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AbortPartitionTransactionHostError::UnknownOperation)?;
        let accepted = matches!(&input, AbortPartitionTransactionInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AbortPartitionTransactionHandoff::HandedOff
        {
            return Err(AbortPartitionTransactionHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AbortPartitionTransactionHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    fn install_effect(
        &mut self,
        index: usize,
        effect: AbortPartitionTransactionEffect,
    ) -> Result<(), AbortPartitionTransactionHostError> {
        let operation_id = self.operations[index].operation_id;
        match effect {
            AbortPartitionTransactionEffect::Submit {
                operation_id: effect_id,
                deadline,
                plan,
            } => {
                if effect_id != operation_id || deadline != self.operations[index].deadline.core() {
                    return Err(AbortPartitionTransactionHostError::SubmissionMismatch);
                }
                self.operations[index].submission = Some(AbortPartitionTransactionSubmission {
                    operation_id,
                    deadline: self.operations[index].deadline,
                    plan,
                });
                self.operations[index].handoff = AbortPartitionTransactionHandoff::Untouched;
                Ok(())
            }
            AbortPartitionTransactionEffect::Complete {
                operation_id: effect_id,
                terminal,
            } => {
                if effect_id != operation_id {
                    return Err(AbortPartitionTransactionHostError::SubmissionMismatch);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
