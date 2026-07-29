//! Exhaustive deterministic-core terminal translation.

use kafka_client_core::{
    DeliveryStatus, UpdateFeatureResult as CoreResult,
    UpdateFeaturesFailureKind as CoreFailureKind, UpdateFeaturesTerminal as CoreTerminal,
};

use super::{
    UpdateFeatureOutcome, UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerError,
    UpdateFeaturesDeliveryStatus, UpdateFeaturesFailure, UpdateFeaturesFailureKind,
    UpdateFeaturesOutcome,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> UpdateFeaturesOutcome {
    match terminal {
        CoreTerminal::Updated(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            UpdateFeaturesOutcome::Updated(UpdateFeaturesBatch {
                throttle_time_ms,
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| {
                        let (feature, result) = outcome.into_parts();
                        UpdateFeatureOutcome {
                            feature,
                            result: match result {
                                CoreResult::Updated => UpdateFeatureResult::Updated,
                                CoreResult::Failed(error) => {
                                    UpdateFeatureResult::Failed(broker_error(error))
                                }
                            },
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => UpdateFeaturesOutcome::Failed(UpdateFeaturesFailure {
            kind: failure_kind(failure.kind()),
            delivery: delivery(failure.delivery()),
        }),
    }
}

fn failure_kind(kind: &CoreFailureKind) -> UpdateFeaturesFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => UpdateFeaturesFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => UpdateFeaturesFailureKind::DriverRejected,
        CoreFailureKind::Transport => UpdateFeaturesFailureKind::Transport,
        CoreFailureKind::Broker(error) => {
            UpdateFeaturesFailureKind::Broker(broker_error(error.clone()))
        }
        CoreFailureKind::ResponseTooLarge => UpdateFeaturesFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => UpdateFeaturesFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => UpdateFeaturesFailureKind::InvalidResponse,
    }
}

fn broker_error(error: kafka_client_core::UpdateFeaturesBrokerError) -> UpdateFeaturesBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    UpdateFeaturesBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn delivery(status: DeliveryStatus) -> UpdateFeaturesDeliveryStatus {
    match status {
        DeliveryStatus::NotSent => UpdateFeaturesDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => UpdateFeaturesDeliveryStatus::PossiblySent,
    }
}
