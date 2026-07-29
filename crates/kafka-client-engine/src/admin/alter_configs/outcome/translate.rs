//! Exhaustive core-to-engine incremental configuration terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, IncrementalAlterConfigResult as CoreConfigResult,
    IncrementalAlterConfigsFailureKind as CoreFailureKind,
    IncrementalAlterConfigsTerminal as CoreTerminal,
};

use super::{
    IncrementalAlterConfigError, IncrementalAlterConfigResult,
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsOutcome,
    IncrementalAlterConfigsResult,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> IncrementalAlterConfigsOutcome {
    match terminal {
        CoreTerminal::Configs(batch) => {
            let (throttle_time_ms, resources) = batch.into_parts();
            IncrementalAlterConfigsOutcome::Configs(IncrementalAlterConfigsResult {
                throttle_time_ms,
                resources: resources
                    .into_iter()
                    .map(|outcome| {
                        let (resource_type, resource_name, result) = outcome.into_resource_parts();
                        let result = match result {
                            CoreConfigResult::Altered => Ok(()),
                            CoreConfigResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(IncrementalAlterConfigError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        IncrementalAlterConfigResult {
                            resource_type,
                            resource_name,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            let kind = match failure.kind() {
                CoreFailureKind::DeadlineElapsed => {
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed
                }
                CoreFailureKind::DriverRejected => {
                    IncrementalAlterConfigsFailureKind::DriverRejected
                }
                CoreFailureKind::Transport => IncrementalAlterConfigsFailureKind::Transport,
                CoreFailureKind::InvalidResponse => {
                    IncrementalAlterConfigsFailureKind::InvalidResponse
                }
                CoreFailureKind::ResponseTooLarge => {
                    IncrementalAlterConfigsFailureKind::ResponseTooLarge
                }
                CoreFailureKind::Compatibility => IncrementalAlterConfigsFailureKind::Compatibility,
            };
            IncrementalAlterConfigsOutcome::Failed(IncrementalAlterConfigsFailure {
                kind,
                delivery: match failure.delivery() {
                    CoreDeliveryStatus::NotSent => IncrementalAlterConfigsDeliveryStatus::NotSent,
                    CoreDeliveryStatus::PossiblySent => {
                        IncrementalAlterConfigsDeliveryStatus::PossiblySent
                    }
                },
            })
        }
    }
}
