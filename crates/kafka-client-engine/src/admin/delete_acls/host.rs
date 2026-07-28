//! Bounded ownership of accepted Admin `DeleteAcls` machines and calls.

mod admission;
mod error;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{DeleteAclsEffect, DeleteAclsInput, Moment, OperationId};

use crate::{
    admin::AdminDeleteAclsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::DeleteAclsCall,
};

pub(crate) use error::DeleteAclsHostError;
pub(crate) use model::{DeleteAclsSubmission, DeleteAclsTurn};

use model::{DeleteAclsHandoff, DeleteAclsOperation};

#[cfg(test)]
use super::DeleteAclsPreparedOutcomes;
use super::{DeleteAclsObserver, DeleteAclsOutcome};

pub(crate) const DELETE_ACLS_CAPACITY: usize = 16;
pub(crate) const DELETE_ACLS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct DeleteAclsAdmission {
    pub(crate) observer: DeleteAclsObserver,
    pub(crate) fault: Option<DeleteAclsHostError>,
}

pub(crate) struct DeleteAclsHost {
    operations: Vec<DeleteAclsOperation>,
    completions: CompletionRegistry<DeleteAclsOutcome, AdminDeleteAclsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DeleteAclsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DeleteAclsHost {
    pub(crate) fn new(publisher: AdminDeleteAclsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DELETE_ACLS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DELETE_ACLS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DELETE_ACLS_CAPACITY),
        }
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<DeleteAclsTurn, DeleteAclsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DeleteAclsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DeleteAclsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DeleteAclsInput::DeadlineElapsed)?;
            return Ok(DeleteAclsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DeleteAclsHostError::MissingSubmission)?;
        self.operations[index].handoff = DeleteAclsHandoff::HandedOff;
        Ok(DeleteAclsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DeleteAclsCall,
    ) -> Result<(), DeleteAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteAclsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteAclsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DeleteAclsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let call = self.operations[index]
            .call
            .as_ref()
            .ok_or(DeleteAclsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_call(call) {
            return Err(DeleteAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DeleteAclsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: kafka_client_core::DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> Result<(), DeleteAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteAclsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DeleteAclsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DeleteAclsHostError::InvalidHandoff);
        }
        if !self.operations[index].matches_evidence(
            &plan,
            request_limit,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        ) {
            return Err(DeleteAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DeleteAclsInput::DriverRejected)
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
        input: DeleteAclsInput,
    ) -> Result<(), DeleteAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DeleteAclsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DeleteAclsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DeleteAclsHandoff::HandedOff {
            return Err(DeleteAclsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DeleteAclsHandoff::Submitted;
        }
        match transition.into_effect() {
            Some(DeleteAclsEffect::Complete {
                operation_id: completed_id,
                terminal,
            }) if completed_id == operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)?;
            }
            Some(_) => return Err(DeleteAclsHostError::SubmissionMismatch),
            None => {}
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }

    #[cfg(test)]
    pub(super) fn prepared_capacities_for_test(&self) -> Option<(usize, usize, usize)> {
        self.operations.first().map(|operation| {
            (
                operation.prepared_results.as_ref().map_or(0, Vec::capacity),
                operation.matching_counts.capacity(),
                operation
                    .prepared_outcomes
                    .as_ref()
                    .map_or(0, DeleteAclsPreparedOutcomes::outcomes_capacity),
            )
        })
    }
}
