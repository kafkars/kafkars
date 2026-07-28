//! Fallible bounded construction of generated caller-ordered ACL creations.

use kafka_wire::{CreateAclsRequest, RetainedSize, create_acls_request::AclCreation};

use super::{
    CreateAclBindingRef,
    retention::{BindingKey, MAX_BINDINGS, MAX_STRING_BYTES, request_peak_charge},
};

/// Invalid binding input or insufficient retained capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsRequestFailure {
    EmptyBatch,
    TooManyBindings { actual: usize, max: usize },
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
    DuplicateBinding,
    RetainedBytes { required: usize, limit: usize },
}

/// Builds API-key 30 input without acquiring route, deadline, or retry authority.
pub(crate) fn create_acls_request(
    bindings: &[CreateAclBindingRef<'_>],
    retained_limit: usize,
) -> Result<CreateAclsRequest, CreateAclsRequestFailure> {
    validate_shape(bindings)?;
    let required = request_peak_charge(bindings).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_unique(bindings, required, retained_limit)?;

    let mut creations = Vec::new();
    creations
        .try_reserve_exact(bindings.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for binding in bindings {
        let mut creation = AclCreation::default();
        creation.resource_type = binding.resource_type();
        creation.resource_name =
            copy_string(binding.resource_name(), required, retained_limit)?.into();
        creation.resource_pattern_type = binding.pattern_type();
        creation.principal = copy_string(binding.principal(), required, retained_limit)?.into();
        creation.host = copy_string(binding.host(), required, retained_limit)?.into();
        creation.operation = binding.operation();
        creation.permission_type = binding.permission_type();
        creations.push(creation);
    }
    let mut request = CreateAclsRequest::default();
    request.creations = creations;
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

fn validate_shape(bindings: &[CreateAclBindingRef<'_>]) -> Result<(), CreateAclsRequestFailure> {
    if bindings.is_empty() {
        return Err(CreateAclsRequestFailure::EmptyBatch);
    }
    if bindings.len() > MAX_BINDINGS {
        return Err(CreateAclsRequestFailure::TooManyBindings {
            actual: bindings.len(),
            max: MAX_BINDINGS,
        });
    }
    for binding in bindings {
        if binding.resource_type() < 2 {
            return Err(CreateAclsRequestFailure::InvalidResourceType {
                actual: binding.resource_type(),
            });
        }
        validate_string(
            binding.resource_name(),
            CreateAclsRequestFailure::EmptyResourceName,
            |actual| CreateAclsRequestFailure::ResourceNameTooLong {
                actual,
                max: MAX_STRING_BYTES,
            },
        )?;
        if binding.pattern_type() < 3 {
            return Err(CreateAclsRequestFailure::InvalidPatternType {
                actual: binding.pattern_type(),
            });
        }
        validate_string(
            binding.principal(),
            CreateAclsRequestFailure::EmptyPrincipal,
            |actual| CreateAclsRequestFailure::PrincipalTooLong {
                actual,
                max: MAX_STRING_BYTES,
            },
        )?;
        validate_string(
            binding.host(),
            CreateAclsRequestFailure::EmptyHost,
            |actual| CreateAclsRequestFailure::HostTooLong {
                actual,
                max: MAX_STRING_BYTES,
            },
        )?;
        if binding.operation() < 2 {
            return Err(CreateAclsRequestFailure::InvalidOperation {
                actual: binding.operation(),
            });
        }
        if binding.permission_type() < 2 {
            return Err(CreateAclsRequestFailure::InvalidPermissionType {
                actual: binding.permission_type(),
            });
        }
    }
    Ok(())
}

fn validate_string(
    value: &str,
    empty: CreateAclsRequestFailure,
    too_long: impl FnOnce(usize) -> CreateAclsRequestFailure,
) -> Result<(), CreateAclsRequestFailure> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_STRING_BYTES {
        return Err(too_long(value.len()));
    }
    Ok(())
}

fn validate_unique(
    bindings: &[CreateAclBindingRef<'_>],
    required: usize,
    limit: usize,
) -> Result<(), CreateAclsRequestFailure> {
    let mut keys = Vec::new();
    keys.try_reserve_exact(bindings.len())
        .map_err(|_| retained_failure(required, limit))?;
    keys.extend(bindings.iter().map(|binding| BindingKey {
        resource_name: binding.resource_name(),
        resource_type: binding.resource_type(),
        pattern_type: binding.pattern_type(),
        principal: binding.principal(),
        host: binding.host(),
        operation: binding.operation(),
        permission_type: binding.permission_type(),
    }));
    keys.sort_unstable();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CreateAclsRequestFailure::DuplicateBinding);
    }
    Ok(())
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, CreateAclsRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), CreateAclsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(CreateAclsRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> CreateAclsRequestFailure {
    CreateAclsRequestFailure::RetainedBytes { required, limit }
}
