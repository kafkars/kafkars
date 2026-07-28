//! Direct normalization into caller-prepared outer and nested terminal vectors.

use core::{mem::size_of, num::NonZeroI16};

use kafka_client_core::{DeleteAclFilterResult, DeleteAclMatchResult, DeleteAclMatchingBinding};
use kafka_wire::{
    DeleteAclsResponse,
    delete_acls_response::{
        DeleteAclsFilterResult as GeneratedFilterResult, DeleteAclsMatchingAcl,
    },
};

use super::{
    model::NormalizedDeleteAclsResponse,
    response_validation::validate_delete_acls_response,
    response_value::{broker_error, copy_string},
};

/// Generated response facts unsafe to bind to caller filter positions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsResponseFailure {
    UnsupportedApiVersion {
        actual: i16,
    },
    EmptyExpectedFilters,
    TooManyExpectedFilters {
        actual: usize,
        max: usize,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    FilterResultCount {
        expected: usize,
        actual: usize,
    },
    FilterErrorWithMatches {
        filter_index: usize,
        actual: usize,
    },
    TooManyMatchesForFilter {
        filter_index: usize,
        actual: usize,
        max: usize,
    },
    TooManyMatchingAcls {
        actual: usize,
        max: usize,
    },
    InvalidResourceType {
        actual: i8,
    },
    EmptyResourceName,
    ResourceNameTooLong {
        actual: usize,
        max: usize,
    },
    InvalidPatternType {
        actual: i8,
    },
    EmptyPrincipal,
    PrincipalTooLong {
        actual: usize,
        max: usize,
    },
    EmptyHost,
    HostTooLong {
        actual: usize,
        max: usize,
    },
    InvalidOperation {
        actual: i8,
    },
    InvalidPermissionType {
        actual: i8,
    },
    DuplicateMatchingAcl {
        filter_index: usize,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    OuterResultStorage,
    MatchingResultStorage {
        filter_index: usize,
    },
    OwnedValueStorage,
}

/// Normalizes into prepared allocations without a second owned result tree.
pub(crate) fn normalize_delete_acls_response(
    selected_version: i16,
    expected_filters: usize,
    response: &DeleteAclsResponse,
    retained_limit: usize,
    mut results: Vec<DeleteAclFilterResult>,
    mut prepare_matching: impl FnMut(usize, usize) -> Result<Vec<DeleteAclMatchingBinding>, ()>,
) -> Result<NormalizedDeleteAclsResponse, DeleteAclsResponseFailure> {
    let validated_peak = validate_delete_acls_response(
        selected_version,
        expected_filters,
        response,
        retained_limit,
        results.capacity(),
    )?;
    require_outer_storage(results.len(), results.capacity(), expected_filters)?;
    let mut terminal_bytes =
        owner_bytes::<DeleteAclFilterResult>(results.capacity(), retained_limit)?;

    for (filter_index, filter) in response.filter_results.iter().enumerate() {
        let (result, added) =
            normalize_filter(filter_index, filter, &mut prepare_matching, retained_limit)?;
        terminal_bytes = add_terminal_bytes(terminal_bytes, added, retained_limit)?;
        results.push(result);
    }

    let retained_bytes = validated_peak.max(terminal_bytes);
    ensure_limit(retained_bytes, retained_limit)?;
    Ok(NormalizedDeleteAclsResponse {
        throttle_time_ms: response.throttle_time_ms as u32,
        results,
        retained_bytes,
    })
}

fn require_outer_storage(
    actual_length: usize,
    actual_capacity: usize,
    expected: usize,
) -> Result<(), DeleteAclsResponseFailure> {
    if actual_length != 0 || actual_capacity < expected {
        return Err(DeleteAclsResponseFailure::OuterResultStorage);
    }
    Ok(())
}

fn normalize_filter(
    filter_index: usize,
    filter: &GeneratedFilterResult,
    prepare_matching: &mut impl FnMut(usize, usize) -> Result<Vec<DeleteAclMatchingBinding>, ()>,
    retained_limit: usize,
) -> Result<(DeleteAclFilterResult, usize), DeleteAclsResponseFailure> {
    if let Some(code) = NonZeroI16::new(filter.error_code) {
        let (error, bytes) = broker_error(code, filter.error_message.as_deref())?;
        return Ok((DeleteAclFilterResult::BrokerFailed(error), bytes));
    }
    let mut matching = prepare_matching(filter_index, filter.matching_acls.len())
        .map_err(|()| DeleteAclsResponseFailure::MatchingResultStorage { filter_index })?;
    if !matching.is_empty() || matching.capacity() < filter.matching_acls.len() {
        return Err(DeleteAclsResponseFailure::MatchingResultStorage { filter_index });
    }
    let mut bytes = owner_bytes::<DeleteAclMatchingBinding>(matching.capacity(), retained_limit)?;
    for source in &filter.matching_acls {
        let (binding, owned_bytes) = normalize_matching(source)?;
        bytes = add_terminal_bytes(bytes, owned_bytes, retained_limit)?;
        matching.push(binding);
    }
    Ok((DeleteAclFilterResult::Matched(matching), bytes))
}

fn normalize_matching(
    source: &DeleteAclsMatchingAcl,
) -> Result<(DeleteAclMatchingBinding, usize), DeleteAclsResponseFailure> {
    let (resource_name, resource_bytes) = copy_string(source.resource_name.as_str())?;
    let (principal, principal_bytes) = copy_string(source.principal.as_str())?;
    let (host, host_bytes) = copy_string(source.host.as_str())?;
    let (result, diagnostic_bytes) = match NonZeroI16::new(source.error_code) {
        Some(code) => {
            let (error, bytes) = broker_error(code, source.error_message.as_deref())?;
            (DeleteAclMatchResult::BrokerFailed(error), bytes)
        }
        None => (DeleteAclMatchResult::Deleted, 0),
    };
    let owned_bytes = resource_bytes
        .checked_add(principal_bytes)
        .and_then(|bytes| bytes.checked_add(host_bytes))
        .and_then(|bytes| bytes.checked_add(diagnostic_bytes))
        .ok_or(DeleteAclsResponseFailure::OwnedValueStorage)?;
    Ok((
        DeleteAclMatchingBinding::new(
            source.resource_type,
            resource_name,
            source.pattern_type,
            principal,
            host,
            source.operation,
            source.permission_type,
            result,
        ),
        owned_bytes,
    ))
}

fn owner_bytes<T>(capacity: usize, limit: usize) -> Result<usize, DeleteAclsResponseFailure> {
    let required = capacity.checked_mul(size_of::<T>()).unwrap_or(usize::MAX);
    ensure_limit(required, limit)?;
    Ok(required)
}

fn add_terminal_bytes(
    current: usize,
    added: usize,
    limit: usize,
) -> Result<usize, DeleteAclsResponseFailure> {
    let required = current.checked_add(added).unwrap_or(usize::MAX);
    ensure_limit(required, limit)?;
    Ok(required)
}

pub(super) fn ensure_limit(required: usize, limit: usize) -> Result<(), DeleteAclsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DeleteAclsResponseFailure::RetainedBytes { required, limit })
}
