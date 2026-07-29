//! Strict selected-v1 normalization of API-key 74 responses.

use kafka_wire::ListConfigResourcesResponse;

use super::{
    ListConfigResourcesResponseFacts,
    materialize::materialize_success,
    retention::{ensure_normalized_limit, error_charge, source_success_charge},
    validation::validate_success_response,
};

/// Compatibility, hostile shape, scalar, allocation, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConfigResourcesProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TooManyResources {
        actual: usize,
        max: usize,
    },
    NonPositiveResourceType {
        actual: i8,
    },
    EmptyResourceName,
    ResourceNameTooLong {
        actual: usize,
        max: usize,
    },
    ResponseTextBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateResource {
        resource_type: i8,
    },
    NormalizedBytesExceeded {
        required: usize,
        max: usize,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates and copies one exact selected-v1 response without generated leakage.
pub(crate) fn normalize_list_config_resources_response(
    selected_version: Option<i16>,
    response: &ListConfigResourcesResponse,
    retained_limit: usize,
) -> Result<ListConfigResourcesResponseFacts, ListConfigResourcesProtocolFailure> {
    let selected_version =
        selected_version.ok_or(ListConfigResourcesProtocolFailure::MissingSelectedVersion)?;
    if selected_version != 1 {
        return Err(ListConfigResourcesProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ListConfigResourcesProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;

    if response.error_code != 0 {
        let required = error_charge();
        ensure_normalized_limit(required, retained_limit)?;
        return Ok(ListConfigResourcesResponseFacts::new(
            throttle_time_ms,
            response.error_code,
            Vec::new(),
            required,
        ));
    }

    validate_success_response(response)?;
    let source_required = source_success_charge(response).unwrap_or(usize::MAX);
    ensure_normalized_limit(source_required, retained_limit)?;
    materialize_success(throttle_time_ms, response, source_required, retained_limit)
}
