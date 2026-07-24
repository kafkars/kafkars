//! Allocation of already-validated normalized `DescribeConfigs` values.

use core::num::NonZeroI16;

use kafka_wire::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
};

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_budget::{MAX_DIAGNOSTIC_BYTES, floor_char_boundary};
use super::describe_configs_model::{
    NormalizedConfigEntry, NormalizedConfigResource, NormalizedConfigResourceError,
    NormalizedConfigSynonym,
};
use super::describe_configs_response::{DescribeConfigsProtocolFailure, matching_config};

pub(super) fn normalize_resource(
    query: &DescribeConfigsQuery<'_>,
    result: &DescribeConfigsResult,
    api_version: i16,
) -> Result<NormalizedConfigResource, DescribeConfigsProtocolFailure> {
    let outcome = if let Some(code) = NonZeroI16::new(result.error_code) {
        let (message, message_truncated) = bounded_diagnostic(result.error_message.as_deref());
        Err(NormalizedConfigResourceError {
            code,
            message,
            message_truncated,
        })
    } else {
        Ok(normalize_configs(query, &result.configs, api_version)?)
    };
    Ok(NormalizedConfigResource {
        resource_type: query.resource_type,
        resource_name: canonical_string(query.resource_name),
        outcome,
    })
}

fn normalize_configs(
    query: &DescribeConfigsQuery<'_>,
    configs: &[DescribeConfigsResourceResult],
    api_version: i16,
) -> Result<Vec<NormalizedConfigEntry>, DescribeConfigsProtocolFailure> {
    let mut normalized = Vec::with_capacity(configs.len());
    if let Some(keys) = query.configuration_keys {
        for key in keys {
            if let Some(config) = matching_config(key, configs)? {
                normalized.push(normalize_config(config, api_version));
            }
        }
    } else {
        normalized.extend(
            configs
                .iter()
                .map(|config| normalize_config(config, api_version)),
        );
        normalized.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    }
    Ok(normalized)
}

fn normalize_config(
    config: &DescribeConfigsResourceResult,
    api_version: i16,
) -> NormalizedConfigEntry {
    let mut synonyms = config
        .synonyms
        .iter()
        .map(normalize_synonym)
        .collect::<Vec<_>>();
    synonyms.sort_unstable_by(|left, right| {
        (&left.name, left.source, &left.value).cmp(&(&right.name, right.source, &right.value))
    });
    NormalizedConfigEntry {
        name: canonical_string(config.name.as_str()),
        value: config.value.as_deref().map(canonical_string),
        read_only: config.read_only,
        source: config.config_source,
        sensitive: config.is_sensitive,
        synonyms,
        config_type: (api_version >= 3).then_some(config.config_type),
        documentation: (api_version >= 3)
            .then(|| config.documentation.as_deref().map(canonical_string))
            .flatten(),
    }
}

fn normalize_synonym(synonym: &DescribeConfigsSynonym) -> NormalizedConfigSynonym {
    NormalizedConfigSynonym {
        name: canonical_string(synonym.name.as_str()),
        value: synonym.value.as_deref().map(canonical_string),
        source: synonym.source,
    }
}

fn bounded_diagnostic(message: Option<&str>) -> (Option<String>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    let retained = floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()));
    (
        Some(canonical_string(&message[..retained])),
        retained < message.len(),
    )
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
