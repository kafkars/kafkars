//! Bounded ownership of accepted incremental configuration machines and bytes.

mod admission;
mod terminal;

use kafka_client_core::{
    IncrementalAlterConfigsEffect, IncrementalAlterConfigsInput, IncrementalAlterConfigsMachine,
    IncrementalAlterConfigsPlan, IncrementalAlterConfigsTerminal, Moment, OperationId,
};

use crate::{
    admin::IncrementalAlterConfigsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
};

use super::{IncrementalAlterConfigsHostError, IncrementalAlterConfigsObserver};

pub(crate) const INCREMENTAL_ALTER_CONFIGS_CAPACITY: usize = 16;
pub(crate) const INCREMENTAL_ALTER_CONFIGS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct IncrementalAlterConfigsAdmission {
    pub(crate) observer: IncrementalAlterConfigsObserver,
    pub(crate) fault: Option<IncrementalAlterConfigsHostError>,
}

pub(crate) struct IncrementalAlterConfigsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) plan: IncrementalAlterConfigsPlan,
    pub(crate) result_limit: usize,
}

impl IncrementalAlterConfigsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        IncrementalAlterConfigsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum IncrementalAlterConfigsTurn {
    Idle,
    Progress,
    Submit(IncrementalAlterConfigsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IncrementalAlterConfigsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct IncrementalAlterConfigsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: IncrementalAlterConfigsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) result_limit: usize,
    pub(super) submission: Option<IncrementalAlterConfigsSubmission>,
    pub(super) handoff: IncrementalAlterConfigsHandoff,
    pub(super) terminal: Option<IncrementalAlterConfigsTerminal>,
}

pub(crate) struct IncrementalAlterConfigsHost {
    pub(super) operations: Vec<IncrementalAlterConfigsOperation>,
    pub(super) completions:
        CompletionRegistry<IncrementalAlterConfigsTerminal, IncrementalAlterConfigsPublisher>,
    pub(super) next_operation_id: Option<OperationId>,
    pub(super) reclaim_pending: Option<CompletionId>,
    pub(super) retained_bytes: usize,
    pub(super) accepting: bool,
    pub(super) health: Option<IncrementalAlterConfigsHostError>,
    pub(super) published_bytes: Vec<(CompletionId, usize)>,
}

impl IncrementalAlterConfigsHost {
    pub(crate) fn new(publisher: IncrementalAlterConfigsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(INCREMENTAL_ALTER_CONFIGS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                INCREMENTAL_ALTER_CONFIGS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(INCREMENTAL_ALTER_CONFIGS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<IncrementalAlterConfigsTurn, IncrementalAlterConfigsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? {
            return Ok(IncrementalAlterConfigsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(IncrementalAlterConfigsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].machine_id();
            self.apply(operation_id, IncrementalAlterConfigsInput::DeadlineElapsed)?;
            return Ok(IncrementalAlterConfigsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(IncrementalAlterConfigsHostError::MissingSubmission)?;
        self.operations[index].mark_handed_off();
        Ok(IncrementalAlterConfigsTurn::Submit(submission))
    }

    pub(crate) fn apply(
        &mut self,
        operation_id: OperationId,
        input: IncrementalAlterConfigsInput,
    ) -> Result<(), IncrementalAlterConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(IncrementalAlterConfigsHostError::UnknownOperation)?;
        let accepted = matches!(&input, IncrementalAlterConfigsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != IncrementalAlterConfigsHandoff::HandedOff {
            return Err(IncrementalAlterConfigsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].mark_submitted();
        }
        if let Some(IncrementalAlterConfigsEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
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

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    pub(super) fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.machine_id() == operation_id)
    }
}

impl IncrementalAlterConfigsOperation {
    pub(super) const fn machine_id(&self) -> OperationId {
        self.operation_id
    }

    fn mark_handed_off(&mut self) {
        self.handoff = IncrementalAlterConfigsHandoff::HandedOff;
    }

    fn mark_submitted(&mut self) {
        self.handoff = IncrementalAlterConfigsHandoff::Submitted;
    }
}
