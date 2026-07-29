//! Semantic normalization for tracked generic `DescribeConfigs` calls.

use kafka_client_core::{
    DeliveryStatus, DescribeConfigBrokerError, DescribeConfigEntry, DescribeConfigOutcome,
    DescribeConfigSynonym, DescribeConfigsBatch, DescribeConfigsInput, DescribeConfigsPlan,
};
use kafka_driver::{ApiVersion, CallFailure, RequestError};
use kafka_wire::DescribeConfigsResponse;

use crate::protocol::admin::describe_configs::{
    DescribeConfigsProtocolFailure, DescribeConfigsQuery, normalize_describe_configs_response,
};

pub(super) fn normalize_terminal(
    plan: &DescribeConfigsPlan,
    result_limit: usize,
    selected_version: Option<ApiVersion>,
    result: Result<DescribeConfigsResponse, RequestError>,
) -> DescribeConfigsInput {
    let response = match result {
        Ok(response) => response,
        Err(
            error @ RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                ..
            },
        ) => {
            return DescribeConfigsInput::DriverDeadlineElapsed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(RequestError::Decode(_error)) => {
            return DescribeConfigsInput::InvalidResponse;
        }
        Err(error) if is_compatibility_failure(&error) => {
            return DescribeConfigsInput::ProtocolIncompatible {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(error) => {
            return DescribeConfigsInput::TransportFailed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
    };
    let Some(version) = selected_version.map(ApiVersion::value) else {
        return DescribeConfigsInput::InvalidResponse;
    };
    let query_keys = plan
        .resources()
        .iter()
        .map(|resource| {
            resource
                .configuration_keys()
                .map(|keys| keys.iter().map(String::as_str).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let queries = plan
        .resources()
        .iter()
        .zip(&query_keys)
        .map(|(resource, keys)| DescribeConfigsQuery {
            resource_type: resource.resource_type(),
            resource_name: resource.resource_name(),
            configuration_keys: keys.as_deref(),
        })
        .collect::<Vec<_>>();
    match normalize_describe_configs_response(&queries, &response, version, result_limit) {
        Ok(normalized) => DescribeConfigsInput::BrokerResponded {
            batch: into_core_batch(normalized),
        },
        Err(DescribeConfigsProtocolFailure::RetainedBytes) => {
            DescribeConfigsInput::ResponseTooLarge
        }
        Err(DescribeConfigsProtocolFailure::ApiVersion) => {
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        Err(_invalid) => DescribeConfigsInput::InvalidResponse,
    }
}

const fn is_compatibility_failure(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Encode(_)
            | RequestError::UnsupportedVersion { .. }
            | RequestError::ApiUnavailable { .. }
            | RequestError::VersionLimitUnavailable { .. }
            | RequestError::VersionFloorUnavailable { .. }
            | RequestError::VersionBoundsInvalid { .. }
    )
}

fn into_core_batch(
    response: crate::protocol::admin::describe_configs::NormalizedDescribeConfigsResponse,
) -> DescribeConfigsBatch {
    DescribeConfigsBatch::new(
        response.throttle_time_ms,
        response
            .resources
            .into_iter()
            .map(|resource| match resource.outcome {
                Ok(configs) => DescribeConfigOutcome::described(
                    resource.resource_type,
                    resource.resource_name,
                    configs.into_iter().map(into_core_entry).collect(),
                ),
                Err(error) => DescribeConfigOutcome::failed(
                    resource.resource_type,
                    resource.resource_name,
                    DescribeConfigBrokerError::new(
                        error.code,
                        error.message,
                        error.message_truncated,
                    ),
                ),
            })
            .collect(),
    )
}

fn into_core_entry(
    entry: crate::protocol::admin::describe_configs::NormalizedConfigEntry,
) -> DescribeConfigEntry {
    DescribeConfigEntry::new(
        entry.name,
        entry.value,
        entry.read_only,
        entry.source,
        entry.sensitive,
        entry
            .synonyms
            .into_iter()
            .map(|synonym| DescribeConfigSynonym::new(synonym.name, synonym.value, synonym.source))
            .collect(),
        entry.config_type,
        entry.documentation,
    )
}
