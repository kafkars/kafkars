//! Fallible copying, duplicate rejection, and canonical ordering for API-key 74.

use kafka_wire::ListConfigResourcesResponse;

use super::{
    ListClientMetricsResourcesProtocolFailure, ListClientMetricsResourcesResponseFacts,
    retention::{ensure_limit, normalized_success_charge},
};

pub(super) fn materialize_success(
    throttle_time_ms: u32,
    response: &ListConfigResourcesResponse,
    source_required: usize,
    retained_limit: usize,
) -> Result<ListClientMetricsResourcesResponseFacts, ListClientMetricsResourcesProtocolFailure> {
    let mut names = Vec::new();
    names
        .try_reserve_exact(response.config_resources.len())
        .map_err(|_| ListClientMetricsResourcesProtocolFailure::Allocation {
            field: "resource_names",
            requested: response.config_resources.len(),
        })?;
    for resource in &response.config_resources {
        names.push(copy_name(resource.resource_name.as_str())?);
    }
    names.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ListClientMetricsResourcesProtocolFailure::DuplicateResourceName);
    }
    let normalized = normalized_success_charge(&names).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    Ok(ListClientMetricsResourcesResponseFacts::new(
        throttle_time_ms,
        0,
        names,
        source_required.max(normalized),
    ))
}

fn copy_name(source: &str) -> Result<String, ListClientMetricsResourcesProtocolFailure> {
    let mut name = String::new();
    name.try_reserve_exact(source.len()).map_err(|_| {
        ListClientMetricsResourcesProtocolFailure::Allocation {
            field: "resource_name",
            requested: source.len(),
        }
    })?;
    name.push_str(source);
    Ok(name)
}
