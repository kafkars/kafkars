//! Bounded ownership of accepted reassignment-listing machines and calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod response;
mod terminal;

use kafka_client_core::{
    ListPartitionReassignmentsEffect, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsPlan, ListPartitionReassignmentsTerminal, Moment, OperationId,
};

use crate::{
    admin::ListPartitionReassignmentsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::{DriverOwner, ListPartitionReassignmentsCall},
};

use super::{ListPartitionReassignmentsHostError, ListPartitionReassignmentsObserver};
use model::{ListPartitionReassignmentsHandoff, ListPartitionReassignmentsOperation};
pub(crate) use model::{ListPartitionReassignmentsSubmission, ListPartitionReassignmentsTurn};

pub(crate) const LIST_PARTITION_REASSIGNMENTS_CAPACITY: usize = 16;
pub(crate) const LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct ListPartitionReassignmentsAdmission {
    pub(crate) observer: ListPartitionReassignmentsObserver,
    pub(crate) fault: Option<ListPartitionReassignmentsHostError>,
}

pub(crate) struct ListPartitionReassignmentsHost {
    operations: Vec<ListPartitionReassignmentsOperation>,
    completions:
        CompletionRegistry<ListPartitionReassignmentsTerminal, ListPartitionReassignmentsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<ListPartitionReassignmentsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl ListPartitionReassignmentsHost {
    pub(crate) fn new(publisher: ListPartitionReassignmentsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(LIST_PARTITION_REASSIGNMENTS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                LIST_PARTITION_REASSIGNMENTS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(LIST_PARTITION_REASSIGNMENTS_CAPACITY),
        }
    }

    #[cfg(test)]
    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<ListPartitionReassignmentsTurn, ListPartitionReassignmentsHostError> {
        self.turn_with_optional_driver(now, None)
    }

    pub(crate) fn turn_with_driver(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<ListPartitionReassignmentsTurn, ListPartitionReassignmentsHostError> {
        self.turn_with_optional_driver(now, Some(driver))
    }

    fn turn_with_optional_driver(
        &mut self,
        now: Moment,
        driver: Option<&DriverOwner>,
    ) -> Result<ListPartitionReassignmentsTurn, ListPartitionReassignmentsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()?
            || self.poll_one_controller_refresh(driver)?
            || self.poll_one_call()?
        {
            return Ok(ListPartitionReassignmentsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(ListPartitionReassignmentsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                ListPartitionReassignmentsInput::DeadlineElapsed,
            )?;
            return Ok(ListPartitionReassignmentsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(ListPartitionReassignmentsHostError::MissingSubmission)?;
        self.operations[index].handoff = ListPartitionReassignmentsHandoff::HandedOff;
        Ok(ListPartitionReassignmentsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: ListPartitionReassignmentsCall,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListPartitionReassignmentsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListPartitionReassignmentsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            ListPartitionReassignmentsInput::DriverAccepted,
        )
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListPartitionReassignmentsHostError::UnknownOperation)?;
        if self.operations[index].handoff != ListPartitionReassignmentsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_submission(&plan, result_limit);
        self.operations[index].rejected_submission = Some((plan, result_limit));
        if !matches {
            return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(ListPartitionReassignmentsInput::DriverRejected)?;
        let terminal = match transition.into_effect() {
            Some(ListPartitionReassignmentsEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => terminal,
            _ => return Err(ListPartitionReassignmentsHostError::MissingTerminal),
        };
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
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
        input: ListPartitionReassignmentsInput,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(ListPartitionReassignmentsHostError::UnknownOperation)?;
        let accepted = matches!(&input, ListPartitionReassignmentsInput::DriverAccepted);
        if accepted
            && self.operations[index].handoff != ListPartitionReassignmentsHandoff::HandedOff
        {
            return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = ListPartitionReassignmentsHandoff::Submitted;
        }
        if let Some(ListPartitionReassignmentsEffect::Complete {
            operation_id: effect_id,
            terminal,
        }) = transition.into_effect()
        {
            if effect_id != operation_id {
                return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
            }
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }
}
