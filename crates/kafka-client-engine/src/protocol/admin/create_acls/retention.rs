//! Checked request, validation-scratch, and borrowed response diagnostic accounting.

use core::mem::size_of;

use kafka_client_core::{CREATE_ACLS_DIAGNOSTIC_BYTES, MAX_CREATE_ACLS_BINDINGS};
use kafka_wire::CreateAclsResponse;

use super::CreateAclBindingRef;

pub(super) const MAX_STRING_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = CREATE_ACLS_DIAGNOSTIC_BYTES;
pub(super) const MAX_BINDINGS: usize = MAX_CREATE_ACLS_BINDINGS;

pub(super) fn request_peak_charge(bindings: &[CreateAclBindingRef<'_>]) -> Option<usize> {
    let owners = bindings
        .len()
        .checked_mul(size_of::<kafka_wire::create_acls_request::AclCreation>())?;
    let scratch = bindings
        .len()
        .checked_mul(size_of::<BindingKey<'static>>())?;
    bindings
        .iter()
        .try_fold(owners.checked_add(scratch)?, |bytes, binding| {
            bytes
                .checked_add(binding.resource_name().len())?
                .checked_add(binding.principal().len())?
                .checked_add(binding.host().len())
        })
}

pub(super) fn response_peak_charge(response: &CreateAclsResponse) -> Option<usize> {
    // The caller separately owns and charges its already-reserved terminal result vector.
    response.results.iter().try_fold(0usize, |bytes, result| {
        bytes.checked_add(bounded_diagnostic_len(result.error_message.as_deref()))
    })
}

pub(super) fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()))
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct BindingKey<'a> {
    pub(super) resource_name: &'a str,
    pub(super) resource_type: i8,
    pub(super) pattern_type: i8,
    pub(super) principal: &'a str,
    pub(super) host: &'a str,
    pub(super) operation: i8,
    pub(super) permission_type: i8,
}
