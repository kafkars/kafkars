//! Checked retained-capacity accounting for normalized API-key 74 v1 facts.

use core::mem::size_of;

use kafka_wire::ListConfigResourcesResponse;

use super::{
    ListConfigResource, ListConfigResourcesProtocolFailure, ListConfigResourcesResponseFacts,
};

pub(super) const MAX_NORMALIZED_BYTES: usize = 2 * 1024 * 1024;

pub(super) const fn error_charge() -> usize {
    size_of::<ListConfigResourcesResponseFacts>()
}

pub(super) fn source_success_charge(response: &ListConfigResourcesResponse) -> Option<usize> {
    let owners = response
        .config_resources
        .len()
        .checked_mul(size_of::<ListConfigResource>())?;
    let text = response
        .config_resources
        .iter()
        .try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.resource_name.as_str().len())
        })?;
    size_of::<ListConfigResourcesResponseFacts>()
        .checked_add(owners)?
        .checked_add(text)
}

pub(super) fn normalized_success_charge(
    resource_capacity: usize,
    resources: &[ListConfigResource],
) -> Option<usize> {
    let owners = resource_capacity.checked_mul(size_of::<ListConfigResource>())?;
    let text = resources.iter().try_fold(0usize, |bytes, resource| {
        bytes.checked_add(resource.resource_name_capacity())
    })?;
    size_of::<ListConfigResourcesResponseFacts>()
        .checked_add(owners)?
        .checked_add(text)
}

pub(super) fn ensure_normalized_limit(
    required: usize,
    caller_limit: usize,
) -> Result<(), ListConfigResourcesProtocolFailure> {
    if required > MAX_NORMALIZED_BYTES {
        return Err(
            ListConfigResourcesProtocolFailure::NormalizedBytesExceeded {
                required,
                max: MAX_NORMALIZED_BYTES,
            },
        );
    }
    if required > caller_limit {
        return Err(ListConfigResourcesProtocolFailure::RetainedBytes {
            required,
            limit: caller_limit,
        });
    }
    Ok(())
}
