//! Bounded ownership of accepted Admin `DescribeLogDirs` machines and calls.

mod admission;
mod response;
mod submission;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine, AdminDescribeLogDirsPlan,
    AdminDescribeLogDirsSelection, AdminDescribeLogDirsTerminal, OperationId,
};

use crate::{
    admin::AdminDescribeLogDirsPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeLogDirsCall, DescribeLogDirsRawTerminal, RecoveredDescribeLogDirsCall},
};

use super::{DescribeLogDirsHostError, DescribeLogDirsObserver};
pub(crate) use submission::DescribeLogDirsTurn;
use submission::{DescribeLogDirsHandoff, DescribeLogDirsSubmission};

pub(crate) const DESCRIBE_LOG_DIRS_CAPACITY: usize = 16;
const DESCRIBE_LOG_DIRS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeLogDirsAdmission {
    pub(crate) observer: DescribeLogDirsObserver,
    pub(crate) fault: Option<DescribeLogDirsHostError>,
}

struct DescribeLogDirsOperation {
    operation_id: OperationId,
    machine: AdminDescribeLogDirsMachine,
    plan: AdminDescribeLogDirsPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    request_scratch_limit: usize,
    result_limit: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeLogDirsSubmission>,
    rejected_submission: Option<(i32, AdminDescribeLogDirsSelection, usize, usize)>,
    handoff: DescribeLogDirsHandoff,
    call: Option<DescribeLogDirsCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredDescribeLogDirsCall>,
    raw_terminal: Option<DescribeLogDirsRawTerminal>,
    terminal: Option<AdminDescribeLogDirsTerminal>,
}

pub(crate) struct DescribeLogDirsHost {
    operations: Vec<DescribeLogDirsOperation>,
    completions: CompletionRegistry<AdminDescribeLogDirsTerminal, AdminDescribeLogDirsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeLogDirsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeLogDirsHost {
    pub(crate) fn new(publisher: AdminDescribeLogDirsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_LOG_DIRS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_LOG_DIRS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_LOG_DIRS_CAPACITY),
        }
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeLogDirsCall,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeLogDirsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
            || self.operations[index].terminal.is_some()
            || self.operations[index].rejected_submission.is_some()
        {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        let operation = &self.operations[index];
        let broker_id = operation
            .machine
            .current_broker()
            .ok_or(DescribeLogDirsHostError::SubmissionMismatch)?;
        let matches = call.matches(
            broker_id,
            operation.plan.selection(),
            operation.request_scratch_limit,
            operation.result_limit,
        );
        self.operations[index].call = Some(call);
        if !matches {
            return Err(DescribeLogDirsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AdminDescribeLogDirsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != DescribeLogDirsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
            || operation.terminal.is_some()
            || operation.rejected_submission.is_some()
        {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        let matches = operation.machine.current_broker() == Some(broker_id)
            && operation.plan.selection() == &selection
            && operation.request_scratch_limit == request_scratch_limit
            && operation.result_limit == result_limit;
        self.operations[index].rejected_submission =
            Some((broker_id, selection, request_scratch_limit, result_limit));
        if !matches {
            return Err(DescribeLogDirsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(AdminDescribeLogDirsInput::DriverRejected)?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        self.validate_effect(index, &effect)?;
        drop(self.operations[index].rejected_submission.take());
        self.install_effect(index, effect)
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
        input: AdminDescribeLogDirsInput,
    ) -> Result<(), DescribeLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeLogDirsHostError::UnknownOperation)?;
        let accepted = matches!(&input, AdminDescribeLogDirsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeLogDirsHandoff::HandedOff {
            return Err(DescribeLogDirsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeLogDirsHandoff::Submitted;
        }
        if let Some(effect) = transition.into_effect() {
            self.install_effect(index, effect)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
