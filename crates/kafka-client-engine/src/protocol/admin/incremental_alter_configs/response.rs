//! Caller-ordered bounded normalization of generated v0/v1 resource results.

use core::num::NonZeroI16;

use kafka_client_core::{
    IncrementalAlterConfigBrokerError, IncrementalAlterConfigOutcome, IncrementalAlterConfigsBatch,
    IncrementalAlterConfigsPlan,
};
use kafka_wire::{
    IncrementalAlterConfigsResponse,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{resource::is_positive_resource_type, retention::ensure_result_fits};
use crate::{
    admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC,
    protocol::admin::result_budget::bounded_message,
};

/// Invalid generated response shape or retained terminal charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncrementalAlterConfigsProtocolFailure {
    /// Ordered results cannot fit the accepted retained-byte reservation.
    RetainedBytes,
    /// Kafka returned a negative throttle duration.
    ThrottleTime,
    /// Kafka returned a different number of resource results.
    ResourceCount,
    /// Kafka returned a nonpositive configuration-resource type.
    NonPositiveResourceType,
    /// Kafka returned an exact type/name identity that was not requested.
    UnexpectedResource,
    /// Kafka omitted one requested exact resource identity.
    MissingResource,
    /// Kafka returned one requested exact resource identity more than once.
    DuplicateResource,
}

/// Converts generated results into caller-ordered, wire-free core facts.
pub(crate) fn normalize_incremental_alter_configs_response_bounded(
    plan: &IncrementalAlterConfigsPlan,
    response: &IncrementalAlterConfigsResponse,
    retained_bytes: usize,
) -> Result<IncrementalAlterConfigsBatch, IncrementalAlterConfigsProtocolFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| IncrementalAlterConfigsProtocolFailure::ThrottleTime)?;
    validate_response_shape(plan, response)?;
    ensure_result_fits(plan, retained_bytes)?;
    let resources = plan
        .resources()
        .iter()
        .map(|resource| {
            let result =
                matching_result(resource.resource_type(), resource.resource_name(), response)?;
            Ok(normalize_resource(
                resource.resource_type(),
                resource.resource_name(),
                result,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IncrementalAlterConfigsBatch::new(
        throttle_time_ms,
        resources,
    ))
}

fn validate_response_shape(
    plan: &IncrementalAlterConfigsPlan,
    response: &IncrementalAlterConfigsResponse,
) -> Result<(), IncrementalAlterConfigsProtocolFailure> {
    if plan.resources().len() != response.responses.len() {
        return Err(IncrementalAlterConfigsProtocolFailure::ResourceCount);
    }
    if response
        .responses
        .iter()
        .any(|result| !is_positive_resource_type(result.resource_type))
    {
        return Err(IncrementalAlterConfigsProtocolFailure::NonPositiveResourceType);
    }
    if response.responses.iter().any(|result| {
        !plan.resources().iter().any(|resource| {
            resource.resource_type() == result.resource_type
                && resource.resource_name() == result.resource_name.as_str()
        })
    }) {
        return Err(IncrementalAlterConfigsProtocolFailure::UnexpectedResource);
    }
    Ok(())
}

fn matching_result<'a>(
    requested_type: i8,
    requested_name: &str,
    response: &'a IncrementalAlterConfigsResponse,
) -> Result<&'a AlterConfigsResourceResponse, IncrementalAlterConfigsProtocolFailure> {
    let mut matches = response.responses.iter().filter(|result| {
        result.resource_type == requested_type && result.resource_name.as_str() == requested_name
    });
    let Some(result) = matches.next() else {
        return Err(IncrementalAlterConfigsProtocolFailure::MissingResource);
    };
    if matches.next().is_some() {
        return Err(IncrementalAlterConfigsProtocolFailure::DuplicateResource);
    }
    Ok(result)
}

fn normalize_resource(
    resource_type: i8,
    resource_name: &str,
    result: &AlterConfigsResourceResponse,
) -> IncrementalAlterConfigOutcome {
    let Some(code) = NonZeroI16::new(result.error_code) else {
        return IncrementalAlterConfigOutcome::resource_altered(resource_type, resource_name);
    };
    #[allow(
        clippy::redundant_closure_for_method_calls,
        reason = "the generated string type is intentionally not a direct engine dependency"
    )]
    let message = result.error_message.as_ref().map(|value| value.as_str());
    let (message, truncated) = bounded_message(message, RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
    IncrementalAlterConfigOutcome::resource_failed(
        resource_type,
        resource_name,
        IncrementalAlterConfigBrokerError::new(code, message, truncated),
    )
}
