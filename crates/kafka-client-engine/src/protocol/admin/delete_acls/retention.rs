//! Checked request, scratch, and caller-prepared terminal byte accounting.

use core::mem::size_of;

use kafka_client_core::{
    DELETE_ACLS_DIAGNOSTIC_BYTES, DeleteAclFilterResult, DeleteAclMatchingBinding,
    MAX_DELETE_ACLS_FILTERS, MAX_DELETE_ACLS_MATCHING_BINDINGS,
};
use kafka_wire::DeleteAclsResponse;

use super::model::DeleteAclsFilterRef;

pub(super) const MAX_FILTER_STRING_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_BINDING_STRING_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = DELETE_ACLS_DIAGNOSTIC_BYTES;
pub(super) const MAX_FILTERS: usize = MAX_DELETE_ACLS_FILTERS;
pub(super) const MAX_MATCHES_PER_FILTER: usize = 64 * 1024;
pub(super) const MAX_TOTAL_MATCHES: usize = MAX_DELETE_ACLS_MATCHING_BINDINGS;

pub(super) fn request_peak_charge(filters: &[DeleteAclsFilterRef<'_>]) -> Option<usize> {
    let owners = filters
        .len()
        .checked_mul(size_of::<kafka_wire::delete_acls_request::DeleteAclsFilter>())?;
    filters.iter().try_fold(owners, |bytes, filter| {
        bytes
            .checked_add(filter.resource_name().map_or(0, str::len))?
            .checked_add(filter.principal().map_or(0, str::len))?
            .checked_add(filter.host().map_or(0, str::len))
    })
}

pub(super) fn response_peak_charge(response: &DeleteAclsResponse) -> Option<usize> {
    let outer = response
        .filter_results
        .len()
        .checked_mul(size_of::<DeleteAclFilterResult>())?;
    let total_matches = response
        .filter_results
        .iter()
        .try_fold(0usize, |total, filter| {
            total.checked_add(filter.matching_acls.len())
        })?;
    let matching_owners = total_matches.checked_mul(size_of::<DeleteAclMatchingBinding>())?;
    let largest_nested = response
        .filter_results
        .iter()
        .map(|filter| filter.matching_acls.len())
        .max()
        .unwrap_or(0);
    let duplicate_scratch = largest_nested.checked_mul(size_of::<MatchingKey<'static>>())?;
    response.filter_results.iter().try_fold(
        outer
            .checked_add(matching_owners)?
            .checked_add(duplicate_scratch)?,
        |bytes, filter| {
            filter.matching_acls.iter().try_fold(
                bytes.checked_add(bounded_diagnostic_len(filter.error_message.as_deref()))?,
                |bytes, matching| {
                    bytes
                        .checked_add(bounded_diagnostic_len(matching.error_message.as_deref()))?
                        .checked_add(matching.resource_name.len())?
                        .checked_add(matching.principal.len())?
                        .checked_add(matching.host.len())
                },
            )
        },
    )
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
pub(super) struct MatchingKey<'a> {
    pub(super) resource_name: &'a str,
    pub(super) resource_type: i8,
    pub(super) pattern_type: i8,
    pub(super) principal: &'a str,
    pub(super) host: &'a str,
    pub(super) operation: i8,
    pub(super) permission_type: i8,
}
