//! Validate-first positional, scalar, count, and duplicate response checks.

use core::mem::size_of;

use kafka_client_core::DeleteAclFilterResult;
use kafka_wire::{
    DeleteAclsResponse,
    delete_acls_response::{
        DeleteAclsFilterResult as GeneratedFilterResult, DeleteAclsMatchingAcl,
    },
};

use super::{
    response::{DeleteAclsResponseFailure, ensure_limit},
    retention::{
        MAX_BINDING_STRING_BYTES, MAX_FILTERS, MAX_MATCHES_PER_FILTER, MAX_TOTAL_MATCHES,
        MatchingKey, response_peak_charge,
    },
    version::supports_delete_acls_version,
};

pub(super) fn validate_delete_acls_response(
    selected_version: i16,
    expected_filters: usize,
    response: &DeleteAclsResponse,
    retained_limit: usize,
    outer_capacity: usize,
) -> Result<usize, DeleteAclsResponseFailure> {
    validate_shape(selected_version, expected_filters, response)?;
    let required = response_peak_charge(response)
        .and_then(|required| {
            outer_capacity
                .saturating_sub(expected_filters)
                .checked_mul(size_of::<DeleteAclFilterResult>())
                .and_then(|extra| required.checked_add(extra))
        })
        .unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_unique_matching(response, required, retained_limit)
}

fn validate_shape(
    selected_version: i16,
    expected_filters: usize,
    response: &DeleteAclsResponse,
) -> Result<(), DeleteAclsResponseFailure> {
    if !supports_delete_acls_version(selected_version) {
        return Err(DeleteAclsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    if expected_filters == 0 {
        return Err(DeleteAclsResponseFailure::EmptyExpectedFilters);
    }
    if expected_filters > MAX_FILTERS {
        return Err(DeleteAclsResponseFailure::TooManyExpectedFilters {
            actual: expected_filters,
            max: MAX_FILTERS,
        });
    }
    if response.throttle_time_ms < 0 {
        return Err(DeleteAclsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    if response.filter_results.len() != expected_filters {
        return Err(DeleteAclsResponseFailure::FilterResultCount {
            expected: expected_filters,
            actual: response.filter_results.len(),
        });
    }
    validate_filter_results(&response.filter_results)
}

fn validate_filter_results(
    filters: &[GeneratedFilterResult],
) -> Result<(), DeleteAclsResponseFailure> {
    let mut total_matches = 0usize;
    for (filter_index, filter) in filters.iter().enumerate() {
        if filter.error_code != 0 && !filter.matching_acls.is_empty() {
            return Err(DeleteAclsResponseFailure::FilterErrorWithMatches {
                filter_index,
                actual: filter.matching_acls.len(),
            });
        }
        if filter.matching_acls.len() > MAX_MATCHES_PER_FILTER {
            return Err(DeleteAclsResponseFailure::TooManyMatchesForFilter {
                filter_index,
                actual: filter.matching_acls.len(),
                max: MAX_MATCHES_PER_FILTER,
            });
        }
        total_matches = total_matches
            .checked_add(filter.matching_acls.len())
            .unwrap_or(usize::MAX);
        if total_matches > MAX_TOTAL_MATCHES {
            return Err(DeleteAclsResponseFailure::TooManyMatchingAcls {
                actual: total_matches,
                max: MAX_TOTAL_MATCHES,
            });
        }
        for matching in &filter.matching_acls {
            validate_matching(matching)?;
        }
    }
    Ok(())
}

fn validate_matching(matching: &DeleteAclsMatchingAcl) -> Result<(), DeleteAclsResponseFailure> {
    if matching.resource_type < 2 {
        return Err(DeleteAclsResponseFailure::InvalidResourceType {
            actual: matching.resource_type,
        });
    }
    validate_string(
        matching.resource_name.as_str(),
        DeleteAclsResponseFailure::EmptyResourceName,
        |actual| DeleteAclsResponseFailure::ResourceNameTooLong {
            actual,
            max: MAX_BINDING_STRING_BYTES,
        },
    )?;
    if matching.pattern_type < 3 {
        return Err(DeleteAclsResponseFailure::InvalidPatternType {
            actual: matching.pattern_type,
        });
    }
    validate_string(
        matching.principal.as_str(),
        DeleteAclsResponseFailure::EmptyPrincipal,
        |actual| DeleteAclsResponseFailure::PrincipalTooLong {
            actual,
            max: MAX_BINDING_STRING_BYTES,
        },
    )?;
    validate_string(
        matching.host.as_str(),
        DeleteAclsResponseFailure::EmptyHost,
        |actual| DeleteAclsResponseFailure::HostTooLong {
            actual,
            max: MAX_BINDING_STRING_BYTES,
        },
    )?;
    if matching.operation < 2 {
        return Err(DeleteAclsResponseFailure::InvalidOperation {
            actual: matching.operation,
        });
    }
    if matching.permission_type < 2 {
        return Err(DeleteAclsResponseFailure::InvalidPermissionType {
            actual: matching.permission_type,
        });
    }
    Ok(())
}

fn validate_string(
    value: &str,
    empty: DeleteAclsResponseFailure,
    too_long: impl FnOnce(usize) -> DeleteAclsResponseFailure,
) -> Result<(), DeleteAclsResponseFailure> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_BINDING_STRING_BYTES {
        return Err(too_long(value.len()));
    }
    Ok(())
}

fn validate_unique_matching(
    response: &DeleteAclsResponse,
    required: usize,
    limit: usize,
) -> Result<usize, DeleteAclsResponseFailure> {
    let largest = response
        .filter_results
        .iter()
        .map(|filter| filter.matching_acls.len())
        .max()
        .unwrap_or(0);
    let requested_scratch = scratch_bytes(largest, limit)?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(largest)
        .map_err(|_| retained_failure(required, limit))?;
    let actual_scratch = scratch_bytes(keys.capacity(), limit)?;
    let adjusted_required = required
        .checked_add(actual_scratch.saturating_sub(requested_scratch))
        .unwrap_or(usize::MAX);
    ensure_limit(adjusted_required, limit)?;

    for (filter_index, filter) in response.filter_results.iter().enumerate() {
        keys.clear();
        for matching in &filter.matching_acls {
            keys.push(matching_key(matching));
        }
        keys.sort_unstable();
        if keys.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DeleteAclsResponseFailure::DuplicateMatchingAcl { filter_index });
        }
    }
    Ok(adjusted_required)
}

fn scratch_bytes(capacity: usize, limit: usize) -> Result<usize, DeleteAclsResponseFailure> {
    capacity
        .checked_mul(size_of::<MatchingKey<'static>>())
        .ok_or_else(|| retained_failure(usize::MAX, limit))
}

fn matching_key(matching: &DeleteAclsMatchingAcl) -> MatchingKey<'_> {
    MatchingKey {
        resource_name: matching.resource_name.as_str(),
        resource_type: matching.resource_type,
        pattern_type: matching.pattern_type,
        principal: matching.principal.as_str(),
        host: matching.host.as_str(),
        operation: matching.operation,
        permission_type: matching.permission_type,
    }
}

const fn retained_failure(required: usize, limit: usize) -> DeleteAclsResponseFailure {
    DeleteAclsResponseFailure::RetainedBytes { required, limit }
}
