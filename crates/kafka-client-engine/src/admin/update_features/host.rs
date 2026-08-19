//! Bounded ownership of accepted finalized-feature machines and concrete calls.

mod admission;
mod model;
mod response;
mod terminal;

#[cfg(test)]
mod ownership_test;
#[cfg(test)]
mod response_test;

use kafka_client_core::{
    Moment, OperationId, UpdateFeaturesEffect, UpdateFeaturesInput, UpdateFeaturesPlan,
    UpdateFeaturesTerminal,
};

use crate::{
    admin::AdminUpdateFeaturesPublisher,
    completion::{CompletionId, CompletionRegistry},
    driver::UpdateFeaturesCall,
};

use super::{UpdateFeaturesHostError, UpdateFeaturesObserver};
use model::{UpdateFeaturesHandoff, UpdateFeaturesOperation};
pub(crate) use model::{UpdateFeaturesSubmission, UpdateFeaturesTurn};

pub(crate) const UPDATE_FEATURES_CAPACITY: usize = 16;
pub(crate) const UPDATE_FEATURES_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct UpdateFeaturesAdmission {
    pub(crate) observer: UpdateFeaturesObserver,
    pub(crate) fault: Option<UpdateFeaturesHostError>,
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
        driver: Option<&crate::driver::DriverOwner>,
    ) -> Result<UpdateFeaturesTurn, UpdateFeaturesHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call(driver)? {
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
            || self.operations[index].recovered_call.is_some()
        {
            return Err(UpdateFeaturesHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let operation = &self.operations[index];
        if !operation.call.as_ref().is_some_and(|call| {
            call.matches_evidence(&operation.response_plan, operation.remaining_result_bytes)
        }) {
            return Err(UpdateFeaturesHostError::SubmissionMismatch);
        }
        self.apply(operation_id, UpdateFeaturesInput::DriverAccepted)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "rejected handoff consumes the exact owned feature-update plan evidence"
    )]
    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: UpdateFeaturesPlan,
        result_limit: usize,
    ) -> Result<(), UpdateFeaturesHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(UpdateFeaturesHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != UpdateFeaturesHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(UpdateFeaturesHostError::InvalidHandoff);
        }
        if operation.response_plan != plan || operation.remaining_result_bytes != result_limit {
            return Err(UpdateFeaturesHostError::SubmissionMismatch);
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
