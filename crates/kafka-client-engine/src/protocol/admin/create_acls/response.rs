//! Strict positional correlation and bounded normalization of generated results.

use kafka_wire::CreateAclsResponse;

use super::{
    NormalizedCreateAclResultRef,
    retention::{MAX_BINDINGS, bounded_diagnostic_len, response_peak_charge},
    version::supports_create_acls_version,
};

/// Generated response facts unsafe to bind to one caller-ordered creation batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    EmptyExpectedResults,
    TooManyExpectedResults { actual: usize, max: usize },
    NegativeThrottleTime { actual: i32 },
    ResultCount { expected: usize, actual: usize },
    RetainedBytes { required: usize, limit: usize },
    ResultStorage,
}

/// Visits every validated response slot in caller order without owning a result vector.
pub(crate) fn normalize_create_acls_response<'a>(
    selected_version: i16,
    expected_results: usize,
    response: &'a CreateAclsResponse,
    retained_limit: usize,
    mut visit: impl FnMut(NormalizedCreateAclResultRef<'a>) -> Result<(), ()>,
) -> Result<(u32, usize), CreateAclsResponseFailure> {
    validate_shape(selected_version, expected_results, response)?;
    let required = response_peak_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    for result in &response.results {
        let retained = bounded_diagnostic_len(result.error_message.as_deref());
        let error_message = result
            .error_message
            .as_deref()
            .map(|message| &message[..retained]);
        visit(NormalizedCreateAclResultRef::new(
            result.error_code,
            error_message,
            result
                .error_message
                .as_ref()
                .is_some_and(|message| retained < message.len()),
        ))
        .map_err(|()| CreateAclsResponseFailure::ResultStorage)?;
    }
    Ok((response.throttle_time_ms as u32, required))
}

fn validate_shape(
    selected_version: i16,
    expected_results: usize,
    response: &CreateAclsResponse,
) -> Result<(), CreateAclsResponseFailure> {
    if !supports_create_acls_version(selected_version) {
        return Err(CreateAclsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    if expected_results == 0 {
        return Err(CreateAclsResponseFailure::EmptyExpectedResults);
    }
    if expected_results > MAX_BINDINGS {
        return Err(CreateAclsResponseFailure::TooManyExpectedResults {
            actual: expected_results,
            max: MAX_BINDINGS,
        });
    }
    if response.throttle_time_ms < 0 {
        return Err(CreateAclsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    if response.results.len() != expected_results {
        return Err(CreateAclsResponseFailure::ResultCount {
            expected: expected_results,
            actual: response.results.len(),
        });
    }
    Ok(())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), CreateAclsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(CreateAclsResponseFailure::RetainedBytes { required, limit })
}
