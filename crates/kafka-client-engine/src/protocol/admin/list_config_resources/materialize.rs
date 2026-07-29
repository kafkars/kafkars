//! Fallible copying, duplicate rejection, and canonical ordering for API key 74 v1.

use kafka_wire::ListConfigResourcesResponse;

use super::{
    ListConfigResource, ListConfigResourcesProtocolFailure, ListConfigResourcesResponseFacts,
    retention::{ensure_normalized_limit, normalized_success_charge},
};

pub(super) fn materialize_success(
    throttle_time_ms: u32,
    response: &ListConfigResourcesResponse,
    source_required: usize,
    retained_limit: usize,
) -> Result<ListConfigResourcesResponseFacts, ListConfigResourcesProtocolFailure> {
    let mut resources = Vec::new();
    resources
        .try_reserve_exact(response.config_resources.len())
        .map_err(|_| ListConfigResourcesProtocolFailure::Allocation {
            field: "resources",
            requested: response.config_resources.len(),
        })?;
    for resource in &response.config_resources {
        resources.push(ListConfigResource::new(
            resource.resource_type,
            copy_name(resource.resource_name.as_str())?,
        ));
    }
    resources.sort_unstable_by(|left, right| {
        left.resource_type()
            .cmp(&right.resource_type())
            .then_with(|| {
                left.resource_name()
                    .as_bytes()
                    .cmp(right.resource_name().as_bytes())
            })
    });
    if let Some(duplicate) = resources.windows(2).find(|pair| {
        pair[0].resource_type() == pair[1].resource_type()
            && pair[0].resource_name() == pair[1].resource_name()
    }) {
        return Err(ListConfigResourcesProtocolFailure::DuplicateResource {
            resource_type: duplicate[0].resource_type(),
        });
    }

    let normalized_required =
        normalized_success_charge(resources.capacity(), &resources).unwrap_or(usize::MAX);
    ensure_normalized_limit(normalized_required, retained_limit)?;
    Ok(ListConfigResourcesResponseFacts::new(
        throttle_time_ms,
        0,
        resources,
        source_required.max(normalized_required),
    ))
}

fn copy_name(source: &str) -> Result<String, ListConfigResourcesProtocolFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        ListConfigResourcesProtocolFailure::Allocation {
            field: "resource_name",
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    Ok(owned)
}
