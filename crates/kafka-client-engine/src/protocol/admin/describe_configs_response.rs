//! Ordered bounded normalization of generated `DescribeConfigs` results.

use kafka_wire::{
    DescribeConfigsResponse,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_budget::ensure_result_fits;
use super::describe_configs_model::NormalizedDescribeConfigsResponse;
use super::describe_configs_values::normalize_resource;

/// Invalid generated response shape or normalized retained-byte charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeConfigsProtocolFailure {
    RetainedBytes,
    ApiVersion,
    ThrottleTime,
    DuplicateRequestedResource,
    DuplicateRequestedConfig,
    ResourceCount,
    UnexpectedResource,
    MissingResource,
    DuplicateResource,
    UnexpectedConfig,
    DuplicateConfig,
}

/// Converts generated results into bounded, deterministic, wire-free facts.
pub(crate) fn normalize_describe_configs_response(
    queries: &[DescribeConfigsQuery<'_>],
    response: &DescribeConfigsResponse,
    api_version: i16,
    retained_bytes: usize,
) -> Result<NormalizedDescribeConfigsResponse, DescribeConfigsProtocolFailure> {
    if !(1..=4).contains(&api_version) {
        return Err(DescribeConfigsProtocolFailure::ApiVersion);
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| DescribeConfigsProtocolFailure::ThrottleTime)?;
    validate_requested_shape(queries)?;
    validate_response_shape(queries, response)?;
    for query in queries {
        let result = matching_result(query, &response.results)?;
        if result.error_code == 0 {
            validate_configs(query, &result.configs)?;
        }
    }
    ensure_result_fits(queries, response, api_version, retained_bytes)?;
    let resources = queries
        .iter()
        .map(|query| {
            normalize_resource(
                query,
                matching_result(query, &response.results)?,
                api_version,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NormalizedDescribeConfigsResponse {
        throttle_time_ms,
        resources,
    })
}

fn validate_requested_shape(
    queries: &[DescribeConfigsQuery<'_>],
) -> Result<(), DescribeConfigsProtocolFailure> {
    for (index, query) in queries.iter().enumerate() {
        if queries[..index].iter().any(|earlier| {
            earlier.resource_type == query.resource_type
                && earlier.resource_name == query.resource_name
        }) {
            return Err(DescribeConfigsProtocolFailure::DuplicateRequestedResource);
        }
        if let Some(keys) = query.configuration_keys {
            for (key_index, key) in keys.iter().enumerate() {
                if keys[..key_index].contains(key) {
                    return Err(DescribeConfigsProtocolFailure::DuplicateRequestedConfig);
                }
            }
        }
    }
    Ok(())
}

fn validate_response_shape(
    queries: &[DescribeConfigsQuery<'_>],
    response: &DescribeConfigsResponse,
) -> Result<(), DescribeConfigsProtocolFailure> {
    if queries.len() != response.results.len() {
        return Err(DescribeConfigsProtocolFailure::ResourceCount);
    }
    if response.results.iter().any(|result| {
        !queries.iter().any(|query| {
            query.resource_type == result.resource_type
                && query.resource_name == result.resource_name.as_str()
        })
    }) {
        return Err(DescribeConfigsProtocolFailure::UnexpectedResource);
    }
    Ok(())
}

pub(super) fn matching_config<'a>(
    requested: &str,
    configs: &'a [DescribeConfigsResourceResult],
) -> Result<Option<&'a DescribeConfigsResourceResult>, DescribeConfigsProtocolFailure> {
    let mut matches = configs
        .iter()
        .filter(|config| config.name.as_str() == requested);
    let config = matches.next();
    if matches.next().is_some() {
        return Err(DescribeConfigsProtocolFailure::DuplicateConfig);
    }
    Ok(config)
}

pub(super) fn matching_result<'a>(
    query: &DescribeConfigsQuery<'_>,
    results: &'a [DescribeConfigsResult],
) -> Result<&'a DescribeConfigsResult, DescribeConfigsProtocolFailure> {
    let mut matches = results.iter().filter(|result| {
        result.resource_type == query.resource_type
            && result.resource_name.as_str() == query.resource_name
    });
    let Some(result) = matches.next() else {
        return Err(DescribeConfigsProtocolFailure::MissingResource);
    };
    if matches.next().is_some() {
        return Err(DescribeConfigsProtocolFailure::DuplicateResource);
    }
    Ok(result)
}

fn validate_configs(
    query: &DescribeConfigsQuery<'_>,
    configs: &[DescribeConfigsResourceResult],
) -> Result<(), DescribeConfigsProtocolFailure> {
    if let Some(keys) = query.configuration_keys {
        if configs
            .iter()
            .any(|config| !keys.contains(&config.name.as_str()))
        {
            return Err(DescribeConfigsProtocolFailure::UnexpectedConfig);
        }
        for key in keys {
            let _config = matching_config(key, configs)?;
        }
        return Ok(());
    }
    for (index, config) in configs.iter().enumerate() {
        if configs[..index]
            .iter()
            .any(|earlier| earlier.name == config.name)
        {
            return Err(DescribeConfigsProtocolFailure::DuplicateConfig);
        }
    }
    Ok(())
}
