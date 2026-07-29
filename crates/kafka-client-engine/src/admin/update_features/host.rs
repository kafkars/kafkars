//! Bounded ownership of accepted finalized-feature machines and concrete calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    Moment, OperationId, UpdateFeaturesEffect, UpdateFeaturesInput, UpdateFeaturesMachine,
    UpdateFeaturesPlan, UpdateFeaturesTerminal,
};

use crate::{
    admin::AdminUpdateFeaturesPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{UpdateFeaturesCall, UpdateFeaturesRawTerminal},
};

use super::{UpdateFeaturesHostError, UpdateFeaturesObserver};

pub(crate) const UPDATE_FEATURES_CAPACITY: usize = 16;
pub(crate) const UPDATE_FEATURES_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct UpdateFeaturesAdmission {
    pub(crate) observer: UpdateFeaturesObserver,
    pub(crate) fault: Option<UpdateFeaturesHostError>,
}

/// Exact validated core plan ready for protocol materialization and handoff.
pub(crate) struct UpdateFeaturesSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: UpdateFeaturesPlan,
    result_limit: usize,
}

impl UpdateFeaturesSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, UpdateFeaturesPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum UpdateFeaturesTurn {
    Idle,
    Progress,
    Submit(UpdateFeaturesSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UpdateFeaturesHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct UpdateFeaturesOperation {
    operation_id: OperationId,
    machine: UpdateFeaturesMachine,
    response_plan: UpdateFeaturesPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<UpdateFeaturesSubmission>,
    handoff: UpdateFeaturesHandoff,
    call: Option<UpdateFeaturesCall>,
    raw_terminal: Option<UpdateFeaturesRawTerminal>,
    terminal: Option<UpdateFeaturesTerminal>,
}

pub(crate) struct UpdateFeaturesHost {
    operations: Vec<UpdateFeaturesOperation>,
    completions: CompletionRegistry<UpdateFeaturesTerminal, AdminUpdateFeaturesPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<UpdateFeaturesHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl UpdateFeaturesHost {
    pub(crate) fn new(publisher: AdminUpdateFeaturesPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(UPDATE_FEATURES_CAPACITY),
            completions: CompletionRegistry::with_publisher(UPDATE_FEATURES_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(UPDATE_FEATURES_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<UpdateFeaturesTurn, UpdateFeaturesHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(UpdateFeaturesTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(UpdateFeaturesTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, UpdateFeaturesInput::DeadlineElapsed)?;
            return Ok(UpdateFeaturesTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(UpdateFeaturesHostError::MissingSubmission)?;
        self.operations[index].handoff = UpdateFeaturesHandoff::HandedOff;
        Ok(UpdateFeaturesTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: UpdateFeaturesCall,
    ) -> Result<(), UpdateFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UpdateFeaturesHostError::UnknownOperation)?;
        if self.operations[index].handoff != UpdateFeaturesHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(UpdateFeaturesHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, UpdateFeaturesInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), UpdateFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UpdateFeaturesHostError::UnknownOperation)?;
        if self.operations[index].handoff != UpdateFeaturesHandoff::HandedOff {
            return Err(UpdateFeaturesHostError::InvalidHandoff);
        }
        self.apply(operation_id, UpdateFeaturesInput::DriverRejected)
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
        input: UpdateFeaturesInput,
    ) -> Result<(), UpdateFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UpdateFeaturesHostError::UnknownOperation)?;
        let accepted = matches!(&input, UpdateFeaturesInput::DriverAccepted);
        if accepted && self.operations[index].handoff != UpdateFeaturesHandoff::HandedOff {
            return Err(UpdateFeaturesHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = UpdateFeaturesHandoff::Submitted;
        }
        if let Some(UpdateFeaturesEffect::Complete { terminal, .. }) = transition.into_effect() {
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
