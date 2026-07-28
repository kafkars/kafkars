//! Bounded ownership of accepted reassignment machines and tracked calls.

mod admission;
mod recovered;
mod response;
mod terminal;

use kafka_client_core::{
    AlterPartitionReassignmentsEffect, AlterPartitionReassignmentsInput,
    AlterPartitionReassignmentsMachine, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsTerminal as CoreTerminal, Moment, OperationId,
};

use crate::{
    admin::AlterPartitionReassignmentsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        AlterPartitionReassignmentsCall, AlterPartitionReassignmentsTerminal, DriverOwner,
        RecoveredAlterPartitionReassignmentsCall,
    },
};

use super::{AlterPartitionReassignmentsHostError, AlterPartitionReassignmentsObserver};

pub(crate) const ALTER_PARTITION_REASSIGNMENTS_CAPACITY: usize = 16;
pub(crate) const ALTER_PARTITION_REASSIGNMENTS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct AlterPartitionReassignmentsAdmission {
    pub(crate) observer: AlterPartitionReassignmentsObserver,
    pub(crate) fault: Option<AlterPartitionReassignmentsHostError>,
}

pub(crate) struct AlterPartitionReassignmentsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: AlterPartitionReassignmentsPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

pub(crate) enum AlterPartitionReassignmentsTurn {
    Idle,
    Progress,
    Submit(AlterPartitionReassignmentsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlterPartitionReassignmentsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct AlterPartitionReassignmentsOperation {
    operation_id: OperationId,
    machine: AlterPartitionReassignmentsMachine,
    response_plan: AlterPartitionReassignmentsPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    request_scratch_limit: usize,
    result_limit: usize,
    submission: Option<AlterPartitionReassignmentsSubmission>,
    handoff: AlterPartitionReassignmentsHandoff,
    call: Option<AlterPartitionReassignmentsCall>,
    recovered_call: Option<RecoveredAlterPartitionReassignmentsCall>,
    raw_terminal: Option<AlterPartitionReassignmentsTerminal>,
    terminal: Option<CoreTerminal>,
}

pub(crate) struct AlterPartitionReassignmentsHost {
    operations: Vec<AlterPartitionReassignmentsOperation>,
    completions: CompletionRegistry<CoreTerminal, AlterPartitionReassignmentsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<AlterPartitionReassignmentsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl AlterPartitionReassignmentsHost {
    pub(crate) fn new(publisher: AlterPartitionReassignmentsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(ALTER_PARTITION_REASSIGNMENTS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                ALTER_PARTITION_REASSIGNMENTS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(ALTER_PARTITION_REASSIGNMENTS_CAPACITY),
        }
    }

    #[cfg(test)]
    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<AlterPartitionReassignmentsTurn, AlterPartitionReassignmentsHostError> {
        self.turn_with_optional_driver(now, None)
    }

    pub(crate) fn turn_with_driver(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<AlterPartitionReassignmentsTurn, AlterPartitionReassignmentsHostError> {
        self.turn_with_optional_driver(now, Some(driver))
    }

    fn turn_with_optional_driver(
        &mut self,
        now: Moment,
        driver: Option<&DriverOwner>,
    ) -> Result<AlterPartitionReassignmentsTurn, AlterPartitionReassignmentsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()?
            || self.poll_one_controller_refresh(driver)?
            || self.poll_one_call()?
        {
            return Ok(AlterPartitionReassignmentsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(AlterPartitionReassignmentsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(
                operation_id,
                AlterPartitionReassignmentsInput::DeadlineElapsed,
            )?;
            return Ok(AlterPartitionReassignmentsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(AlterPartitionReassignmentsHostError::MissingSubmission)?;
        self.operations[index].handoff = AlterPartitionReassignmentsHandoff::HandedOff;
        Ok(AlterPartitionReassignmentsTurn::Submit(submission))
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
        input: AlterPartitionReassignmentsInput,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterPartitionReassignmentsHostError::UnknownOperation)?;
        let transition = self.operations[index].machine.apply(input)?;
        if let Some(AlterPartitionReassignmentsEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }
}
