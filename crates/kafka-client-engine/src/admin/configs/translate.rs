//! Exhaustive translation of closed core terminals into stable engine values.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeConfigResult as CoreConfigResult,
    DescribeConfigsFailureKind as CoreFailureKind, DescribeConfigsTerminal,
};

use super::outcome::{
    DescribeConfigEntry, DescribeConfigResourceError, DescribeConfigResourceResult,
    DescribeConfigSynonym, DescribeConfigsBatch, DescribeConfigsDeliveryStatus,
    DescribeConfigsFailure, DescribeConfigsFailureKind, DescribeConfigsOutcome,
};

pub(crate) fn translate_terminal(terminal: DescribeConfigsTerminal) -> DescribeConfigsOutcome {
    match terminal {
        DescribeConfigsTerminal::Configs(batch) => {
            let (throttle_time_ms, resources) = batch.into_parts();
            DescribeConfigsOutcome::Configs(DescribeConfigsBatch {
                throttle_time_ms,
                resources: resources
                    .into_iter()
                    .map(|resource| {
                        let (resource_type, resource_name, result) = resource.into_parts();
                        let result = match result {
                            CoreConfigResult::Configs(configs) => {
                                Ok(configs.into_iter().map(translate_entry).collect())
                            }
                            CoreConfigResult::Failed(error) => {
                                let (code, message, message_truncated) = error.into_parts();
                                Err(DescribeConfigResourceError {
                                    code,
                                    message,
                                    message_truncated,
                                })
                            }
                        };
                        DescribeConfigResourceResult {
                            resource_type,
                            resource_name,
                            result,
                        }
                    })
                    .collect(),
            })
        }
        DescribeConfigsTerminal::Failed(failure) => {
            DescribeConfigsOutcome::Failed(DescribeConfigsFailure {
                kind: translate_failure(failure.kind()),
                delivery: translate_delivery(failure.delivery()),
            })
        }
    }
}

fn translate_entry(entry: kafka_client_core::DescribeConfigEntry) -> DescribeConfigEntry {
    let (name, value, read_only, source, sensitive, synonyms, config_type, documentation) =
        entry.into_parts();
    DescribeConfigEntry {
        name,
        value,
        read_only,
        source,
        sensitive,
        synonyms: synonyms
            .into_iter()
            .map(|synonym| {
                let (name, value, source) = synonym.into_parts();
                DescribeConfigSynonym {
                    name,
                    value,
                    source,
                }
            })
            .collect(),
        config_type,
        documentation,
    }
}

const fn translate_failure(kind: CoreFailureKind) -> DescribeConfigsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeConfigsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeConfigsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeConfigsFailureKind::Transport,
        CoreFailureKind::InvalidResponse => DescribeConfigsFailureKind::InvalidResponse,
        CoreFailureKind::ResponseTooLarge => DescribeConfigsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeConfigsFailureKind::Compatibility,
    }
}

const fn translate_delivery(delivery: CoreDeliveryStatus) -> DescribeConfigsDeliveryStatus {
    match delivery {
        CoreDeliveryStatus::NotSent => DescribeConfigsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeConfigsDeliveryStatus::PossiblySent,
    }
}
