//! Validate-first bounded normalization of generated ACL response facts.

use kafka_wire::{
    DescribeAclsResponse,
    describe_acls_response::{AclDescription, DescribeAclsResource},
};

use super::{
    NormalizedAclBinding, NormalizedDescribeAclsResponse,
    retention::{
        BindingRef, MAX_ACLS, MAX_HOST_BYTES, MAX_PRINCIPAL_BYTES, MAX_RESOURCE_NAME_BYTES,
        MAX_RESOURCES, ResourceKey, bounded_diagnostic_len, normalized_retained_charge,
        response_peak_charge,
    },
    version::supports_describe_acls_version,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeAclsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    ResourcesWithTopLevelError { actual: usize },
    TooManyResources { actual: usize, max: usize },
    EmptyResourceName,
    ResourceNameTooLong { actual: usize, max: usize },
    EmptyResourceAcls,
    TooManyAcls { actual: usize, max: usize },
    EmptyPrincipal,
    PrincipalTooLong { actual: usize, max: usize },
    EmptyHost,
    HostTooLong { actual: usize, max: usize },
    DuplicateResource,
    DuplicateAcl,
    RetainedBytes { required: usize, limit: usize },
}

pub(crate) fn normalize_describe_acls_response(
    selected_version: i16,
    response: &DescribeAclsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeAclsResponse, DescribeAclsResponseFailure> {
    validate_scalar_shape(selected_version, response)?;
    validate_bounded_shape(response)?;
    let required = response_peak_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let bindings = sorted_borrowed_bindings(response, required, retained_limit)?;
    let error_message_len = bounded_diagnostic_len(response.error_message.as_deref());
    let error_message = response
        .error_message
        .as_deref()
        .map(|message| copy_string(&message[..error_message_len], required, retained_limit))
        .transpose()?;
    let mut normalized_bindings = Vec::new();
    normalized_bindings
        .try_reserve_exact(bindings.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for binding in bindings {
        normalized_bindings.push(NormalizedAclBinding {
            resource_type: binding.resource_type,
            resource_name: copy_string(binding.resource_name, required, retained_limit)?,
            pattern_type: binding.pattern_type,
            principal: copy_string(binding.principal, required, retained_limit)?,
            host: copy_string(binding.host, required, retained_limit)?,
            operation: binding.operation,
            permission_type: binding.permission_type,
        });
    }
    let mut normalized = NormalizedDescribeAclsResponse {
        throttle_time_ms: response.throttle_time_ms as u32,
        error_code: response.error_code,
        error_message,
        error_message_truncated: response
            .error_message
            .as_ref()
            .is_some_and(|message| error_message_len < message.len()),
        bindings: normalized_bindings,
        retained_bytes: 0,
    };
    let retained = normalized_retained_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, retained_limit)?;
    normalized.retained_bytes = required;
    Ok(normalized)
}

fn validate_scalar_shape(
    selected_version: i16,
    response: &DescribeAclsResponse,
) -> Result<(), DescribeAclsResponseFailure> {
    if !supports_describe_acls_version(selected_version) {
        return Err(DescribeAclsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    if response.throttle_time_ms < 0 {
        return Err(DescribeAclsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    if response.error_code != 0 && !response.resources.is_empty() {
        return Err(DescribeAclsResponseFailure::ResourcesWithTopLevelError {
            actual: response.resources.len(),
        });
    }
    Ok(())
}

fn validate_bounded_shape(
    response: &DescribeAclsResponse,
) -> Result<(), DescribeAclsResponseFailure> {
    if response.resources.len() > MAX_RESOURCES {
        return Err(DescribeAclsResponseFailure::TooManyResources {
            actual: response.resources.len(),
            max: MAX_RESOURCES,
        });
    }
    let mut acl_count = 0usize;
    for resource in &response.resources {
        validate_resource(resource)?;
        acl_count = acl_count
            .checked_add(resource.acls.len())
            .unwrap_or(usize::MAX);
        if acl_count > MAX_ACLS {
            return Err(DescribeAclsResponseFailure::TooManyAcls {
                actual: acl_count,
                max: MAX_ACLS,
            });
        }
        for acl in &resource.acls {
            validate_acl(acl)?;
        }
    }
    Ok(())
}

fn validate_resource(resource: &DescribeAclsResource) -> Result<(), DescribeAclsResponseFailure> {
    if resource.resource_name.is_empty() {
        return Err(DescribeAclsResponseFailure::EmptyResourceName);
    }
    if resource.resource_name.len() > MAX_RESOURCE_NAME_BYTES {
        return Err(DescribeAclsResponseFailure::ResourceNameTooLong {
            actual: resource.resource_name.len(),
            max: MAX_RESOURCE_NAME_BYTES,
        });
    }
    if resource.acls.is_empty() {
        return Err(DescribeAclsResponseFailure::EmptyResourceAcls);
    }
    Ok(())
}

fn validate_acl(acl: &AclDescription) -> Result<(), DescribeAclsResponseFailure> {
    if acl.principal.is_empty() {
        return Err(DescribeAclsResponseFailure::EmptyPrincipal);
    }
    if acl.principal.len() > MAX_PRINCIPAL_BYTES {
        return Err(DescribeAclsResponseFailure::PrincipalTooLong {
            actual: acl.principal.len(),
            max: MAX_PRINCIPAL_BYTES,
        });
    }
    if acl.host.is_empty() {
        return Err(DescribeAclsResponseFailure::EmptyHost);
    }
    if acl.host.len() > MAX_HOST_BYTES {
        return Err(DescribeAclsResponseFailure::HostTooLong {
            actual: acl.host.len(),
            max: MAX_HOST_BYTES,
        });
    }
    Ok(())
}

fn sorted_borrowed_bindings<'a>(
    response: &'a DescribeAclsResponse,
    required: usize,
    limit: usize,
) -> Result<Vec<BindingRef<'a>>, DescribeAclsResponseFailure> {
    let mut resource_keys = Vec::new();
    resource_keys
        .try_reserve_exact(response.resources.len())
        .map_err(|_| retained_failure(required, limit))?;
    let acl_count = response
        .resources
        .iter()
        .fold(0, |count, resource| count + resource.acls.len());
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(acl_count)
        .map_err(|_| retained_failure(required, limit))?;
    for resource in &response.resources {
        let resource_name = resource.resource_name.as_str();
        resource_keys.push(ResourceKey {
            resource_name,
            resource_type: resource.resource_type,
            pattern_type: resource.pattern_type,
        });
        bindings.extend(resource.acls.iter().map(|acl| BindingRef {
            resource_name,
            resource_type: resource.resource_type,
            pattern_type: resource.pattern_type,
            principal: acl.principal.as_str(),
            host: acl.host.as_str(),
            operation: acl.operation,
            permission_type: acl.permission_type,
        }));
    }
    resource_keys.sort_unstable();
    if resource_keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeAclsResponseFailure::DuplicateResource);
    }
    bindings.sort_unstable();
    if bindings.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeAclsResponseFailure::DuplicateAcl);
    }
    Ok(bindings)
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeAclsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeAclsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeAclsResponseFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeAclsResponseFailure {
    DescribeAclsResponseFailure::RetainedBytes { required, limit }
}
