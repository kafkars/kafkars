//! Checked request, validation-scratch, and normalized-result byte accounting.

use core::mem::size_of;

use kafka_wire::DescribeAclsResponse;

use super::{DescribeAclsFilterRef, NormalizedAclBinding, NormalizedDescribeAclsResponse};

pub(super) const MAX_FILTER_STRING_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_RESOURCE_NAME_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_PRINCIPAL_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_HOST_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;
pub(super) const MAX_RESOURCES: usize = 16 * 1024;
pub(super) const MAX_ACLS: usize = 1024 * 1024;

pub(super) fn request_retained_charge(filter: DescribeAclsFilterRef<'_>) -> Option<usize> {
    filter
        .resource_name()
        .map_or(0, str::len)
        .checked_add(filter.principal().map_or(0, str::len))?
        .checked_add(filter.host().map_or(0, str::len))
}

pub(super) fn response_peak_charge(response: &DescribeAclsResponse) -> Option<usize> {
    let acl_count = response
        .resources
        .iter()
        .try_fold(0usize, |count, resource| {
            count.checked_add(resource.acls.len())
        })?;
    let output_owners = acl_count.checked_mul(size_of::<NormalizedAclBinding>())?;
    let resource_scratch = response
        .resources
        .len()
        .checked_mul(size_of::<ResourceKey<'static>>())?;
    let acl_scratch = acl_count.checked_mul(size_of::<BindingRef<'static>>())?;
    let output_text = response.resources.iter().try_fold(
        bounded_diagnostic_len(response.error_message.as_deref()),
        |bytes, resource| {
            resource.acls.iter().try_fold(bytes, |bytes, acl| {
                bytes
                    .checked_add(resource.resource_name.len())?
                    .checked_add(acl.principal.len())?
                    .checked_add(acl.host.len())
            })
        },
    )?;
    size_of::<NormalizedDescribeAclsResponse>()
        .checked_add(output_owners)?
        .checked_add(output_text)?
        .checked_add(resource_scratch)?
        .checked_add(acl_scratch)
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedDescribeAclsResponse,
) -> Option<usize> {
    response.bindings.iter().try_fold(
        size_of::<NormalizedDescribeAclsResponse>()
            .checked_add(
                response
                    .bindings
                    .capacity()
                    .checked_mul(size_of::<NormalizedAclBinding>())?,
            )?
            .checked_add(response.error_message.as_ref().map_or(0, String::capacity))?,
        |bytes, binding| {
            bytes
                .checked_add(binding.resource_name.capacity())?
                .checked_add(binding.principal.capacity())?
                .checked_add(binding.host.capacity())
        },
    )
}

pub(super) fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()))
}

pub(super) fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ResourceKey<'a> {
    pub(super) resource_name: &'a str,
    pub(super) resource_type: i8,
    pub(super) pattern_type: i8,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct BindingRef<'a> {
    pub(super) resource_name: &'a str,
    pub(super) resource_type: i8,
    pub(super) pattern_type: i8,
    pub(super) principal: &'a str,
    pub(super) host: &'a str,
    pub(super) operation: i8,
    pub(super) permission_type: i8,
}
