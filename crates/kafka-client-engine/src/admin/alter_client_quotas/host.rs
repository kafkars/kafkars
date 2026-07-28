//! Bounded ownership of accepted Admin `AlterClientQuotas` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AlterClientQuotasEffect, AlterClientQuotasInput, AlterClientQuotasMachine,
    AlterClientQuotasPlan, AlterClientQuotasTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminAlterClientQuotasPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{AlterClientQuotasCall, AlterClientQuotasRawTerminal},
};

use super::{AlterClientQuotasHostError, AlterClientQuotasObserver};

pub(crate) const ALTER_CLIENT_QUOTAS_CAPACITY: usize = 16;
pub(crate) const ALTER_CLIENT_QUOTAS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AlterClientQuotasAdmission {
    pub(crate) observer: AlterClientQuotasObserver,
    pub(crate) fault: Option<AlterClientQuotasHostError>,
}

/// One exact alteration plan ready for the engine's driver-admission stage.
pub(crate) struct AlterClientQuotasSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: AlterClientQuotasPlan,
    result_limit: usize,
}

impl AlterClientQuotasSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (OperationId, OperationDeadline, AlterClientQuotasPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum AlterClientQuotasTurn {
    Idle,
    Progress,
    Submit(AlterClientQuotasSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlterClientQuotasHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AlterClientQuotasOperation {
    operation_id: OperationId,
    machine: AlterClientQuotasMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<AlterClientQuotasSubmission>,
    handoff: AlterClientQuotasHandoff,
    call: Option<AlterClientQuotasCall>,
    raw_terminal: Option<AlterClientQuotasRawTerminal>,
    terminal: Option<AlterClientQuotasTerminal>,
}

pub(crate) struct AlterClientQuotasHost {
    operations: Vec<AlterClientQuotasOperation>,
    completions: CompletionRegistry<AlterClientQuotasTerminal, AdminAlterClientQuotasPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterClientQuotasHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterClientQuotasHost {
    pub(crate) fn new(publisher: AdminAlterClientQuotasPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_CLIENT_QUOTAS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_CLIENT_QUOTAS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_CLIENT_QUOTAS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterClientQuotasTurn, AlterClientQuotasHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(AlterClientQuotasTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterClientQuotasTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AlterClientQuotasInput::DeadlineElapsed)?;
            return Ok(AlterClientQuotasTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterClientQuotasHostError::MissingSubmission)?;
        self.operations[index].handoff = AlterClientQuotasHandoff::HandedOff;
        Ok(AlterClientQuotasTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AlterClientQuotasCall,
    ) -> Result<(), AlterClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterClientQuotasHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterClientQuotasHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(AlterClientQuotasHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, AlterClientQuotasInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), AlterClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterClientQuotasHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterClientQuotasHandoff::HandedOff {
            return Err(AlterClientQuotasHostError::InvalidHandoff);
        }
        self.apply(operation_id, AlterClientQuotasInput::DriverRejected)
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
        input: AlterClientQuotasInput,
    ) -> Result<(), AlterClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterClientQuotasHostError::UnknownOperation)?;
        let accepted = matches!(&input, AlterClientQuotasInput::DriverAccepted);
        if accepted && self.operations[index].handoff != AlterClientQuotasHandoff::HandedOff {
            return Err(AlterClientQuotasHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = AlterClientQuotasHandoff::Submitted;
        }
        if let Some(AlterClientQuotasEffect::Complete { terminal, .. }) = transition.into_effect() {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
