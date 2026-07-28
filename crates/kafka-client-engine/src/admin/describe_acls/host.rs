//! Bounded ownership of accepted Admin `DescribeAcls` machines and calls.

mod admission;
mod model;
#[cfg(test)]
mod ownership_test;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeAclsEffect, DescribeAclsInput, DescribeAclsPlan, DescribeAclsTerminal, Moment,
    OperationId,
};

use crate::{
    admin::AdminDescribeAclsPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::DescribeAclsCall,
};

use super::{DescribeAclsHostError, DescribeAclsObserver};
use model::{DescribeAclsHandoff, DescribeAclsOperation};
pub(crate) use model::{DescribeAclsSubmission, DescribeAclsTurn};

pub(crate) const DESCRIBE_ACLS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_ACLS_RETAINED_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct DescribeAclsAdmission {
    pub(crate) observer: DescribeAclsObserver,
    pub(crate) fault: Option<DescribeAclsHostError>,
}

pub(crate) struct DescribeAclsHost {
    operations: Vec<DescribeAclsOperation>,
    completions: CompletionRegistry<DescribeAclsTerminal, AdminDescribeAclsPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeAclsHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeAclsHost {
    pub(crate) fn new(publisher: AdminDescribeAclsPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_ACLS_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_ACLS_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_ACLS_CAPACITY),
        }
    }

    pub(crate) fn turn(&mut self, now: Moment) -> Result<DescribeAclsTurn, DescribeAclsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeAclsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeAclsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeAclsInput::DeadlineElapsed)?;
            return Ok(DescribeAclsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeAclsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeAclsHandoff::HandedOff;
        Ok(DescribeAclsTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeAclsCall,
    ) -> Result<(), DescribeAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeAclsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeAclsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeAclsHostError::InvalidHandoff);
        }
        let matches = self.operations[index].matches_call(&call);
        self.operations[index].call = Some(call);
        if !matches {
            return Err(DescribeAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DescribeAclsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: DescribeAclsPlan,
        result_limit: usize,
    ) -> Result<(), DescribeAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeAclsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeAclsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeAclsHostError::InvalidHandoff);
        }
        if !self.operations[index].matches_submission(&plan, result_limit) {
            return Err(DescribeAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DescribeAclsInput::DriverRejected)
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
        input: DescribeAclsInput,
    ) -> Result<(), DescribeAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeAclsHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeAclsInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeAclsHandoff::HandedOff {
            return Err(DescribeAclsHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeAclsHandoff::Submitted;
        }
        if let Some(DescribeAclsEffect::Complete {
            operation_id: completed_id,
            terminal,
        }) = transition.into_effect()
        {
            if completed_id != operation_id {
                return Err(DescribeAclsHostError::SubmissionMismatch);
            }
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
