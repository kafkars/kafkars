//! Retained-result accounting for normalized `DescribeConfigs` facts.

use core::mem::size_of;

use kafka_wire::describe_configs_response::DescribeConfigsResourceResult;

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_model::{
    NormalizedConfigEntry, NormalizedConfigResource, NormalizedConfigSynonym,
    NormalizedDescribeConfigsResponse,
};
use super::describe_configs_response::{
    DescribeConfigsProtocolFailure, matching_config, matching_result,
};

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn ensure_result_fits(
    queries: &[DescribeConfigsQuery<'_>],
    response: &kafka_wire::DescribeConfigsResponse,
    api_version: i16,
    retained_bytes: usize,
) -> Result<(), DescribeConfigsProtocolFailure> {
    let required = required_retained_bytes(queries, response, api_version)?;
    if required > retained_bytes {
        return Err(DescribeConfigsProtocolFailure::RetainedBytes);
    }
    Ok(())
}

pub(super) fn required_retained_bytes(
    queries: &[DescribeConfigsQuery<'_>],
    response: &kafka_wire::DescribeConfigsResponse,
    api_version: i16,
) -> Result<usize, DescribeConfigsProtocolFailure> {
    let resources = queries
        .len()
        .checked_mul(size_of::<NormalizedConfigResource>())
        .and_then(|bytes| bytes.checked_add(size_of::<NormalizedDescribeConfigsResponse>()))
        .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?;
    queries.iter().try_fold(resources, |bytes, query| {
        let result = matching_result(query, &response.results)?;
        let bytes = bytes
            .checked_add(query.resource_name.len())
            .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?;
        if result.error_code == 0 {
            add_success_bytes(bytes, query, result, api_version)
        } else {
            let message = result.error_message.as_deref().map_or(0, |value| {
                floor_char_boundary(value, MAX_DIAGNOSTIC_BYTES.min(value.len()))
            });
            bytes
                .checked_add(message)
                .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)
        }
    })
}

fn add_success_bytes(
    bytes: usize,
    query: &DescribeConfigsQuery<'_>,
    result: &kafka_wire::describe_configs_response::DescribeConfigsResult,
    api_version: i16,
) -> Result<usize, DescribeConfigsProtocolFailure> {
    let bytes = bytes
        .checked_add(
            result
                .configs
                .len()
                .checked_mul(size_of::<NormalizedConfigEntry>())
                .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?,
        )
        .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?;
    match query.configuration_keys {
        Some(keys) => keys.iter().try_fold(bytes, |bytes, key| {
            let Some(config) = matching_config(key, &result.configs)? else {
                return Ok(bytes);
            };
            add_config_bytes(bytes, config, api_version)
        }),
        None => result.configs.iter().try_fold(bytes, |bytes, config| {
            add_config_bytes(bytes, config, api_version)
        }),
    }
}

fn add_config_bytes(
    bytes: usize,
    config: &DescribeConfigsResourceResult,
    api_version: i16,
) -> Result<usize, DescribeConfigsProtocolFailure> {
    let text_bytes = config
        .name
        .len()
        .checked_add(config.value.as_deref().map_or(0, str::len))
        .and_then(|value| {
            value.checked_add(if api_version >= 3 {
                config.documentation.as_deref().map_or(0, str::len)
            } else {
                0
            })
        })
        .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?;
    let synonym_owners = config
        .synonyms
        .len()
        .checked_mul(size_of::<NormalizedConfigSynonym>())
        .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?;
    config.synonyms.iter().try_fold(
        bytes
            .checked_add(text_bytes)
            .and_then(|value| value.checked_add(synonym_owners))
            .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)?,
        |bytes, synonym| {
            bytes
                .checked_add(synonym.name.len())
                .and_then(|value| value.checked_add(synonym.value.as_deref().map_or(0, str::len)))
                .ok_or(DescribeConfigsProtocolFailure::RetainedBytes)
        },
    )
}

pub(super) fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}
