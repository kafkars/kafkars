//! Bounded ownership of feature machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeFeaturesEffect, DescribeFeaturesInput, DescribeFeaturesMachine,
    DescribeFeaturesTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeFeaturesPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeFeaturesCall, DescribeFeaturesRawTerminal, RecoveredDescribeFeaturesCall},
};

use super::{DescribeFeaturesHostError, DescribeFeaturesObserver};

use model::DescribeFeaturesHandoff;
pub(crate) use model::{DescribeFeaturesSubmission, DescribeFeaturesTurn};

pub(crate) const DESCRIBE_FEATURES_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_FEATURES_RESULT_BYTES: usize =
    kafka_client_core::DESCRIBE_FEATURES_MAX_RETAINED_BYTES;
/// Aggregate bytes retained by all active and published feature descriptions.
pub(crate) const DESCRIBE_FEATURES_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeFeaturesAdmission {
    pub(crate) observer: DescribeFeaturesObserver,
    pub(crate) fault: Option<DescribeFeaturesHostError>,
}

struct DescribeFeaturesOperation {
    operation_id: OperationId,
    machine: DescribeFeaturesMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeFeaturesSubmission>,
    handoff: DescribeFeaturesHandoff,
    call: Option<DescribeFeaturesCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredDescribeFeaturesCall>,
    raw_terminal: Option<DescribeFeaturesRawTerminal>,
    terminal: Option<DescribeFeaturesTerminal>,
}

pub(crate) struct DescribeFeaturesHost {
    operations: Vec<DescribeFeaturesOperation>,
    completions: CompletionRegistry<DescribeFeaturesTerminal, AdminDescribeFeaturesPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeFeaturesHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeFeaturesHost {
    pub(crate) fn new(publisher: AdminDescribeFeaturesPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_FEATURES_CAPACITY),
            completions: CompletionRegistry::with_publisher(DESCRIBE_FEATURES_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_FEATURES_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeFeaturesTurn, DescribeFeaturesHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeFeaturesTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeFeaturesTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeFeaturesInput::DeadlineElapsed)?;
            return Ok(DescribeFeaturesTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeFeaturesHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeFeaturesHandoff::HandedOff;
        Ok(DescribeFeaturesTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeFeaturesCall,
    ) -> Result<(), DescribeFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeFeaturesHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeFeaturesHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
        {
            return Err(DescribeFeaturesHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeFeaturesInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeFeaturesHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeFeaturesHandoff::HandedOff
            || self.operations[index].recovered_call.is_some()
        {
            return Err(DescribeFeaturesHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeFeaturesInput::DriverRejected)
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
        input: DescribeFeaturesInput,
    ) -> Result<(), DescribeFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeFeaturesHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeFeaturesInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeFeaturesHandoff::HandedOff {
            return Err(DescribeFeaturesHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeFeaturesHandoff::Submitted;
        }
        if let Some(DescribeFeaturesEffect::Complete { terminal, .. }) = transition.into_effect() {
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
