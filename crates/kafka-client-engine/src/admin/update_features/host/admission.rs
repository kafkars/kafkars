//! Atomic terminal, plan, and four-MiB envelope reservation.

use core::mem::size_of;

use kafka_client_core::{
    Moment, OperationId, UpdateFeature, UpdateFeaturesEffect, UpdateFeaturesInput,
    UpdateFeaturesMachine, UpdateFeaturesPlan,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    UPDATE_FEATURES_CAPACITY, UPDATE_FEATURES_RETAINED_BYTES, UpdateFeaturesAdmission,
    UpdateFeaturesHandoff, UpdateFeaturesHost, UpdateFeaturesHostError, UpdateFeaturesOperation,
    UpdateFeaturesSubmission,
};
use crate::admin::update_features::{UpdateFeaturesAdmissionErrorKind, UpdateFeaturesObserver};

impl UpdateFeaturesHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: UpdateFeaturesPlan,
    ) -> Result<UpdateFeaturesAdmission, UpdateFeaturesAdmissionErrorKind> {
        if !self.accepting {
            return Err(UpdateFeaturesAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= UPDATE_FEATURES_CAPACITY {
            return Err(UpdateFeaturesAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(UpdateFeaturesAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(UpdateFeaturesAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = UPDATE_FEATURES_RETAINED_BYTES
            .checked_sub(owner_charge)
            .filter(|limit| *limit > 0)
            .ok_or(UpdateFeaturesAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(UPDATE_FEATURES_RETAINED_BYTES)
            .filter(|total| *total <= UPDATE_FEATURES_RETAINED_BYTES)
            .ok_or(UpdateFeaturesAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let response_plan = plan.clone();
        let mut operation = UpdateFeaturesOperation {
            operation_id,
            machine: UpdateFeaturesMachine::new(operation_id, deadline.core(), plan),
            response_plan,
            completion_id,
            deadline,
            retained_bytes: UPDATE_FEATURES_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: UpdateFeaturesHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now, deadline);
        let terminal_ready = matches!(start_result, Ok(true));
        let mut fault = start_result.err();
        if let Some(error) = fault {
            self.health = Some(error);
        }
        self.operations.push(operation);
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(UpdateFeaturesAdmission {
            observer: UpdateFeaturesObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut UpdateFeaturesOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, UpdateFeaturesHostError> {
    let transition = operation
        .machine
        .apply(UpdateFeaturesInput::Start { now })?;
    match transition.into_effect() {
        Some(UpdateFeaturesEffect::Submit {
            operation_id,
            deadline: core_deadline,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(UpdateFeaturesHostError::SubmissionMismatch);
            }
            operation.submission = Some(UpdateFeaturesSubmission {
                operation_id,
                deadline,
                plan,
                result_limit: operation.remaining_result_bytes,
            });
            Ok(false)
        }
        Some(UpdateFeaturesEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(UpdateFeaturesHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(UpdateFeaturesHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> UpdateFeaturesAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => UpdateFeaturesAdmissionErrorKind::Capacity,
        _ => UpdateFeaturesAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &UpdateFeaturesPlan) -> Option<usize> {
    let update_storage = plan
        .updates()
        .len()
        .checked_mul(size_of::<UpdateFeature>())?;
    let feature_bytes = plan.updates().iter().try_fold(0usize, |bytes, update| {
        bytes.checked_add(update.feature().len())
    })?;
    let one_plan = update_storage.checked_add(feature_bytes)?;
    size_of::<UpdateFeaturesOperation>()
        .checked_add(size_of::<UpdateFeaturesSubmission>())?
        .checked_add(one_plan.checked_mul(3)?)
}
