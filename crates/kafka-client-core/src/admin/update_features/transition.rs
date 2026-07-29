//! Atomic feature-update transitions, response correlation, and terminal assignment.

use std::collections::BTreeMap;

use crate::DeliveryStatus;

use super::{
    UPDATE_FEATURES_DIAGNOSTIC_BYTES, UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES, UpdateFeatureOutcome,
    UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerError,
    UpdateFeaturesBrokerResponse, UpdateFeaturesEffect, UpdateFeaturesFailure,
    UpdateFeaturesFailureKind, UpdateFeaturesInput, UpdateFeaturesMachine,
    UpdateFeaturesMachineError, UpdateFeaturesState, UpdateFeaturesTerminal,
    UpdateFeaturesTransition,
};

impl UpdateFeaturesMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: UpdateFeaturesInput,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state == UpdateFeaturesState::Completed {
            return Err(UpdateFeaturesMachineError::AlreadyCompleted);
        }
        match input {
            UpdateFeaturesInput::Start { now } => self.start(now),
            UpdateFeaturesInput::DriverAccepted => self.driver_accepted(),
            UpdateFeaturesInput::DriverRejected => self.finish_awaiting(
                UpdateFeaturesFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            UpdateFeaturesInput::DeadlineElapsed => self.finish_awaiting(
                UpdateFeaturesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            UpdateFeaturesInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(UpdateFeaturesFailureKind::DeadlineElapsed, delivery)
            }
            UpdateFeaturesInput::BrokerResponded { response } => self.broker_responded(response),
            UpdateFeaturesInput::BrokerRejected { error } => self.broker_rejected(error),
            UpdateFeaturesInput::ResponseTooLarge => self.finish_submitted(
                UpdateFeaturesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            UpdateFeaturesInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(UpdateFeaturesFailureKind::Compatibility, delivery)
            }
            UpdateFeaturesInput::TransportFailed { delivery } => {
                self.finish_submitted(UpdateFeaturesFailureKind::Transport, delivery)
            }
            UpdateFeaturesInput::InvalidResponse => self.finish_submitted(
                UpdateFeaturesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state != UpdateFeaturesState::Ready {
            return Err(UpdateFeaturesMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                UpdateFeaturesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = UpdateFeaturesState::AwaitingDriver;
        Ok(UpdateFeaturesTransition::one(
            UpdateFeaturesEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(&mut self) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state != UpdateFeaturesState::AwaitingDriver {
            return Err(UpdateFeaturesMachineError::InvalidState);
        }
        self.state = UpdateFeaturesState::Submitted;
        Ok(UpdateFeaturesTransition::none())
    }

    fn broker_responded(
        &mut self,
        response: UpdateFeaturesBrokerResponse,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state != UpdateFeaturesState::Submitted {
            return Err(UpdateFeaturesMachineError::InvalidState);
        }
        let batch = match response {
            UpdateFeaturesBrokerResponse::FeatureResults(batch) => {
                let Some(batch) = self.correlate_batch(batch) else {
                    return Ok(self.finish_failure(
                        UpdateFeaturesFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    ));
                };
                batch
            }
            UpdateFeaturesBrokerResponse::AtomicSuccess { throttle_time_ms } => {
                self.synthesize_atomic_success(throttle_time_ms)
            }
        };
        Ok(self.finish(UpdateFeaturesTerminal::Updated(batch)))
    }

    fn correlate_batch(&self, batch: UpdateFeaturesBatch) -> Option<UpdateFeaturesBatch> {
        let (throttle_time_ms, outcomes) = batch.into_parts();
        if outcomes.len() != self.plan.updates().len() {
            return None;
        }
        let mut by_feature = BTreeMap::new();
        for outcome in outcomes {
            let (feature, result) = outcome.into_parts();
            if feature.is_empty()
                || feature.len() > UPDATE_FEATURES_MAX_FEATURE_NAME_BYTES
                || !result_has_bounded_diagnostic(&result)
                || by_feature.insert(feature, result).is_some()
            {
                return None;
            }
        }
        let mut ordered = Vec::with_capacity(self.plan.updates().len());
        for update in self.plan.updates() {
            let feature = update.feature().to_owned();
            let result = by_feature.remove(update.feature())?;
            ordered.push(match result {
                UpdateFeatureResult::Updated => UpdateFeatureOutcome::updated(feature),
                UpdateFeatureResult::Failed(error) => UpdateFeatureOutcome::failed(feature, error),
            });
        }
        if !by_feature.is_empty() {
            return None;
        }
        Some(UpdateFeaturesBatch::new(throttle_time_ms, ordered))
    }

    fn synthesize_atomic_success(&self, throttle_time_ms: u32) -> UpdateFeaturesBatch {
        let outcomes = self
            .plan
            .updates()
            .iter()
            .map(|update| UpdateFeatureOutcome::updated(update.feature().to_owned()))
            .collect();
        UpdateFeaturesBatch::new(throttle_time_ms, outcomes)
    }

    fn broker_rejected(
        &mut self,
        error: UpdateFeaturesBrokerError,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if !error_has_bounded_diagnostic(&error) {
            return self.finish_submitted(
                UpdateFeaturesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            );
        }
        self.finish_submitted(
            UpdateFeaturesFailureKind::Broker(error),
            DeliveryStatus::PossiblySent,
        )
    }

    fn finish_awaiting(
        &mut self,
        kind: UpdateFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state != UpdateFeaturesState::AwaitingDriver {
            return Err(UpdateFeaturesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: UpdateFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<UpdateFeaturesTransition, UpdateFeaturesMachineError> {
        if self.state != UpdateFeaturesState::Submitted {
            return Err(UpdateFeaturesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: UpdateFeaturesFailureKind,
        delivery: DeliveryStatus,
    ) -> UpdateFeaturesTransition {
        self.finish(UpdateFeaturesTerminal::Failed(UpdateFeaturesFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: UpdateFeaturesTerminal) -> UpdateFeaturesTransition {
        self.state = UpdateFeaturesState::Completed;
        UpdateFeaturesTransition::one(UpdateFeaturesEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn result_has_bounded_diagnostic(result: &UpdateFeatureResult) -> bool {
    match result {
        UpdateFeatureResult::Updated => true,
        UpdateFeatureResult::Failed(error) => error_has_bounded_diagnostic(error),
    }
}

fn error_has_bounded_diagnostic(error: &UpdateFeaturesBrokerError) -> bool {
    error
        .message()
        .is_none_or(|message| message.len() <= UPDATE_FEATURES_DIAGNOSTIC_BYTES)
}
