//! Fallible bounded construction of one generated ACL filter request.

use kafka_wire::{DescribeAclsRequest, RetainedSize};

use super::{
    DescribeAclsFilterRef,
    retention::{MAX_FILTER_STRING_BYTES, request_retained_charge},
};

/// Invalid filter text or insufficient retained capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeAclsRequestFailure {
    EmptyResourceName,
    ResourceNameTooLong { actual: usize, max: usize },
    EmptyPrincipal,
    PrincipalTooLong { actual: usize, max: usize },
    EmptyHost,
    HostTooLong { actual: usize, max: usize },
    RetainedBytes { required: usize, limit: usize },
}

/// Builds one API-key 29 request without acquiring route or deadline authority.
pub(crate) fn describe_acls_request(
    filter: DescribeAclsFilterRef<'_>,
    retained_limit: usize,
) -> Result<DescribeAclsRequest, DescribeAclsRequestFailure> {
    validate_optional(
        filter.resource_name(),
        DescribeAclsRequestFailure::EmptyResourceName,
        |actual| DescribeAclsRequestFailure::ResourceNameTooLong {
            actual,
            max: MAX_FILTER_STRING_BYTES,
        },
    )?;
    validate_optional(
        filter.principal(),
        DescribeAclsRequestFailure::EmptyPrincipal,
        |actual| DescribeAclsRequestFailure::PrincipalTooLong {
            actual,
            max: MAX_FILTER_STRING_BYTES,
        },
    )?;
    validate_optional(
        filter.host(),
        DescribeAclsRequestFailure::EmptyHost,
        |actual| DescribeAclsRequestFailure::HostTooLong {
            actual,
            max: MAX_FILTER_STRING_BYTES,
        },
    )?;
    let required = request_retained_charge(filter).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = filter.resource_type();
    request.resource_name_filter = copy_optional(filter.resource_name(), required, retained_limit)?;
    request.pattern_type_filter = filter.pattern_type();
    request.principal_filter = copy_optional(filter.principal(), required, retained_limit)?;
    request.host_filter = copy_optional(filter.host(), required, retained_limit)?;
    request.operation = filter.operation();
    request.permission_type = filter.permission_type();
    let actual = request.retained_size().heap_bytes();
    ensure_limit(actual, retained_limit)?;
    Ok(request)
}

fn validate_optional(
    value: Option<&str>,
    empty: DescribeAclsRequestFailure,
    too_long: impl FnOnce(usize) -> DescribeAclsRequestFailure,
) -> Result<(), DescribeAclsRequestFailure> {
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
) -> Result<Option<kafka_wire_core::StrBytes>, DescribeAclsRequestFailure> {
    source
        .map(|source| copy_string(source, required, limit).map(Into::into))
        .transpose()
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeAclsRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeAclsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeAclsRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeAclsRequestFailure {
    DescribeAclsRequestFailure::RetainedBytes { required, limit }
}
