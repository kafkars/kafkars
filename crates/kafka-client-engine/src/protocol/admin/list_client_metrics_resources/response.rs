//! Validate-first normalization of generated flexible-v0 API-key 74 responses.

use kafka_wire::ListConfigResourcesResponse;

use super::{
    ListClientMetricsResourcesResponseFacts,
    materialize::materialize_success,
    retention::{ensure_limit, error_charge, source_success_charge},
    validation::validate_response,
};

/// Compatibility, hostile shape, allocation, scalar, or capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListClientMetricsResourcesProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    SuccessPayloadWithBrokerError,
    TooManyResources {
        actual: usize,
        max: usize,
    },
    UnexpectedResourceType {
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
    DuplicateResourceName,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates and copies one exact selected-v0 response without generated leakage.
pub(crate) fn normalize_list_client_metrics_resources_response(
    selected_version: Option<i16>,
    response: &ListConfigResourcesResponse,
    retained_limit: usize,
) -> Result<ListClientMetricsResourcesResponseFacts, ListClientMetricsResourcesProtocolFailure> {
    let selected_version = selected_version
        .ok_or(ListClientMetricsResourcesProtocolFailure::MissingSelectedVersion)?;
    if selected_version != 0 {
        return Err(
            ListClientMetricsResourcesProtocolFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ListClientMetricsResourcesProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_response(response)?;
    if response.error_code != 0 {
        let required = error_charge();
        ensure_limit(required, retained_limit)?;
        return Ok(ListClientMetricsResourcesResponseFacts::new(
            throttle_time_ms,
            response.error_code,
            Vec::new(),
            required,
        ));
    }
    let required = source_success_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    materialize_success(throttle_time_ms, response, required, retained_limit)
}
