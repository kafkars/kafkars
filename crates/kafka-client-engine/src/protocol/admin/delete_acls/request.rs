//! Fallible bounded construction of generated caller-ordered ACL filters.

use kafka_wire::{
    DeleteAclsRequest, RetainedSize, delete_acls_request::DeleteAclsFilter as GeneratedFilter,
};

use super::{
    model::DeleteAclsFilterRef,
    retention::{MAX_FILTER_STRING_BYTES, MAX_FILTERS, request_peak_charge},
};

/// Invalid filter input or insufficient capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsRequestFailure {
    EmptyBatch,
    TooManyFilters { actual: usize, max: usize },
    InvalidResourceType { actual: i8 },
    EmptyResourceName,
    ResourceNameTooLong { actual: usize, max: usize },
    InvalidPatternType { actual: i8 },
    EmptyPrincipal,
    PrincipalTooLong { actual: usize, max: usize },
    EmptyHost,
    HostTooLong { actual: usize, max: usize },
    InvalidOperation { actual: i8 },
    InvalidPermissionType { actual: i8 },
    RetainedBytes { required: usize, limit: usize },
}

/// Builds API-key 31 input without route, deadline, or retry authority.
pub(crate) fn delete_acls_request(
    filters: &[DeleteAclsFilterRef<'_>],
    retained_limit: usize,
) -> Result<DeleteAclsRequest, DeleteAclsRequestFailure> {
    validate_shape(filters)?;
    let required = request_peak_charge(filters).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    let mut generated = Vec::new();
    generated
        .try_reserve_exact(filters.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for filter in filters {
        let mut value = GeneratedFilter::default();
        value.resource_type_filter = filter.resource_type();
        value.resource_name_filter =
            copy_optional(filter.resource_name(), required, retained_limit)?;
        value.pattern_type_filter = filter.pattern_type();
        value.principal_filter = copy_optional(filter.principal(), required, retained_limit)?;
        value.host_filter = copy_optional(filter.host(), required, retained_limit)?;
        value.operation = filter.operation();
        value.permission_type = filter.permission_type();
        generated.push(value);
    }
    let mut request = DeleteAclsRequest::default();
    request.filters = generated;
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

fn validate_shape(filters: &[DeleteAclsFilterRef<'_>]) -> Result<(), DeleteAclsRequestFailure> {
    if filters.is_empty() {
        return Err(DeleteAclsRequestFailure::EmptyBatch);
    }
    if filters.len() > MAX_FILTERS {
        return Err(DeleteAclsRequestFailure::TooManyFilters {
            actual: filters.len(),
            max: MAX_FILTERS,
        });
    }
    for filter in filters {
        if filter.resource_type() <= 0 {
            return Err(DeleteAclsRequestFailure::InvalidResourceType {
                actual: filter.resource_type(),
            });
        }
        validate_optional_string(
            filter.resource_name(),
            DeleteAclsRequestFailure::EmptyResourceName,
            |actual| DeleteAclsRequestFailure::ResourceNameTooLong {
                actual,
                max: MAX_FILTER_STRING_BYTES,
            },
        )?;
        if filter.pattern_type() <= 0 {
            return Err(DeleteAclsRequestFailure::InvalidPatternType {
                actual: filter.pattern_type(),
            });
        }
        validate_optional_string(
            filter.principal(),
            DeleteAclsRequestFailure::EmptyPrincipal,
            |actual| DeleteAclsRequestFailure::PrincipalTooLong {
                actual,
                max: MAX_FILTER_STRING_BYTES,
            },
        )?;
        validate_optional_string(
            filter.host(),
            DeleteAclsRequestFailure::EmptyHost,
            |actual| DeleteAclsRequestFailure::HostTooLong {
                actual,
                max: MAX_FILTER_STRING_BYTES,
            },
        )?;
        if filter.operation() <= 0 {
            return Err(DeleteAclsRequestFailure::InvalidOperation {
                actual: filter.operation(),
            });
        }
        if filter.permission_type() <= 0 {
            return Err(DeleteAclsRequestFailure::InvalidPermissionType {
                actual: filter.permission_type(),
            });
        }
    }
    Ok(())
}

fn validate_optional_string(
    value: Option<&str>,
    empty: DeleteAclsRequestFailure,
    too_long: impl FnOnce(usize) -> DeleteAclsRequestFailure,
) -> Result<(), DeleteAclsRequestFailure> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_FILTER_STRING_BYTES {
        return Err(too_long(value.len()));
    }
    Ok(())
}

fn copy_optional(
    source: Option<&str>,
    required: usize,
    limit: usize,
) -> Result<Option<kafka_wire_core::StrBytes>, DeleteAclsRequestFailure> {
    source
        .map(|source| copy_string(source, required, limit).map(Into::into))
        .transpose()
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DeleteAclsRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DeleteAclsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DeleteAclsRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DeleteAclsRequestFailure {
    DeleteAclsRequestFailure::RetainedBytes { required, limit }
}
