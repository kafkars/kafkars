//! Bounded ownership of legacy resource-replacement machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

use kafka_client_core::{
    LegacyAlterConfigsEffect, LegacyAlterConfigsInput, LegacyAlterConfigsPlan,
    LegacyAlterConfigsTerminal, Moment, OperationId,
};

use crate::{
    admin::LegacyAlterConfigsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::LegacyAlterConfigsCall,
};

use super::{LegacyAlterConfigsHostError, LegacyAlterConfigsObserver};

use model::{LegacyAlterConfigsHandoff, LegacyAlterConfigsOperation};
pub(crate) use model::{LegacyAlterConfigsSubmission, LegacyAlterConfigsTurn};

pub(crate) const LEGACY_ALTER_CONFIGS_CAPACITY: usize = 16;
pub(crate) const LEGACY_ALTER_CONFIGS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct LegacyAlterConfigsAdmission {
    pub(crate) observer: LegacyAlterConfigsObserver,
    pub(crate) fault: Option<LegacyAlterConfigsHostError>,
}

pub(crate) struct LegacyAlterConfigsHost {
    operations: Vec<LegacyAlterConfigsOperation>,
    completions: CompletionRegistry<LegacyAlterConfigsTerminal, LegacyAlterConfigsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<LegacyAlterConfigsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl LegacyAlterConfigsHost {
    pub(crate) fn new(publisher: LegacyAlterConfigsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LEGACY_ALTER_CONFIGS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LEGACY_ALTER_CONFIGS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LEGACY_ALTER_CONFIGS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<LegacyAlterConfigsTurn, LegacyAlterConfigsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(LegacyAlterConfigsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(LegacyAlterConfigsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, LegacyAlterConfigsInput::DeadlineElapsed)?;
            return Ok(LegacyAlterConfigsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(LegacyAlterConfigsHostError::MissingSubmission)?;
        self.operations[index].handoff = LegacyAlterConfigsHandoff::HandedOff;
        Ok(LegacyAlterConfigsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: LegacyAlterConfigsCall,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(LegacyAlterConfigsHostError::UnknownOperation)?;
        if self.operations[index].handoff != LegacyAlterConfigsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(LegacyAlterConfigsHostError::InvalidHandoff);
        }
        if !self.operations[index].matches_correlation(call.route(), call.plan()) {
            return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, LegacyAlterConfigsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        route: kafka_client_core::LegacyAlterConfigsRoute,
        plan: LegacyAlterConfigsPlan,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(LegacyAlterConfigsHostError::UnknownOperation)?;
        if self.operations[index].handoff != LegacyAlterConfigsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(LegacyAlterConfigsHostError::InvalidHandoff);
        }
        if !self.operations[index].matches_correlation(route, &plan) {
            return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, LegacyAlterConfigsInput::DriverRejected)
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
        input: LegacyAlterConfigsInput,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(LegacyAlterConfigsHostError::UnknownOperation)?;
        let accepted = matches!(&input, LegacyAlterConfigsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != LegacyAlterConfigsHandoff::HandedOff {
            return Err(LegacyAlterConfigsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = LegacyAlterConfigsHandoff::Submitted;
        }
        self.apply_effect(index, transition.into_effect())
    }

    pub(super) fn apply_effect(
        &mut self,
        index: usize,
        effect: Option<LegacyAlterConfigsEffect>,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        match effect {
            Some(LegacyAlterConfigsEffect::Submit {
                operation_id,
                deadline,
                route,
                plan,
            }) => {
                if operation_id != self.operations[index].operation_id
                    || deadline != self.operations[index].deadline.core()
                {
                    return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
                }
                self.operations[index].prepare_submission(route, plan)?;
            }
            Some(LegacyAlterConfigsEffect::Complete {
                operation_id,
                terminal,
            }) => {
                if operation_id != self.operations[index].operation_id {
                    return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
                }
                if matches!(&terminal, LegacyAlterConfigsTerminal::Configs(_))
                    && self.operations[index].remaining_result_bytes != 0
                {
                    return Err(LegacyAlterConfigsHostError::ByteAccounting);
                }
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)?;
            }
            None => {}
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn apply_input_for_test(
        &mut self,
        operation_id: OperationId,
        input: LegacyAlterConfigsInput,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        self.apply(operation_id, input)
    }
}
