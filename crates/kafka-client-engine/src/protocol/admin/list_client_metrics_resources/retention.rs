//! Checked retained-capacity accounting for normalized API-key 74 facts.

use core::mem::size_of;

use kafka_wire::ListConfigResourcesResponse;

use super::{ListClientMetricsResourcesProtocolFailure, ListClientMetricsResourcesResponseFacts};

pub(super) const fn error_charge() -> usize {
    size_of::<ListClientMetricsResourcesResponseFacts>()
}

pub(super) fn source_success_charge(response: &ListConfigResourcesResponse) -> Option<usize> {
    let owners = response
        .config_resources
        .len()
        .checked_mul(size_of::<String>())?;
    let text = response
        .config_resources
        .iter()
        .try_fold(0usize, |bytes, resource| {
            bytes.checked_add(resource.resource_name.len())
        })?;
    size_of::<ListClientMetricsResourcesResponseFacts>()
        .checked_add(owners)?
        .checked_add(text)
}

pub(super) fn normalized_success_charge(resource_names: &[String]) -> Option<usize> {
    let owners = resource_names.len().checked_mul(size_of::<String>())?;
    let text = resource_names
        .iter()
        .try_fold(0usize, |bytes, name| bytes.checked_add(name.capacity()))?;
    size_of::<ListClientMetricsResourcesResponseFacts>()
        .checked_add(owners)?
        .checked_add(text)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), ListClientMetricsResourcesProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(ListClientMetricsResourcesProtocolFailure::RetainedBytes { required, limit })
}
