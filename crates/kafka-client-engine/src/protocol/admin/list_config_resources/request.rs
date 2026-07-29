//! Bounded canonical request construction for flexible API-key 74 v1.

use kafka_wire::ListConfigResourcesRequest;

pub(super) const MAX_REQUEST_RESOURCE_TYPES: usize = 32;

/// Invalid resource-type selection or allocation failure before driver admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConfigResourcesRequestFailure {
    TooManyResourceTypes { actual: usize, max: usize },
    NonPositiveResourceType { actual: i8 },
    DuplicateResourceType { actual: i8 },
    Allocation { requested: usize },
}

/// Builds a v1 request; an empty selection asks Kafka for all supported types.
pub(crate) fn list_config_resources_request(
    resource_types: &[i8],
) -> Result<ListConfigResourcesRequest, ListConfigResourcesRequestFailure> {
    validate_resource_types(resource_types)?;
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(resource_types.len())
        .map_err(|_| ListConfigResourcesRequestFailure::Allocation {
            requested: resource_types.len(),
        })?;
    canonical.extend_from_slice(resource_types);
    canonical.sort_unstable();

    let mut request = ListConfigResourcesRequest::default();
    request.resource_types = canonical;
    Ok(request)
}

fn validate_resource_types(resource_types: &[i8]) -> Result<(), ListConfigResourcesRequestFailure> {
    if resource_types.len() > MAX_REQUEST_RESOURCE_TYPES {
        return Err(ListConfigResourcesRequestFailure::TooManyResourceTypes {
            actual: resource_types.len(),
            max: MAX_REQUEST_RESOURCE_TYPES,
        });
    }
    for (index, resource_type) in resource_types.iter().copied().enumerate() {
        if resource_type <= 0 {
            return Err(ListConfigResourcesRequestFailure::NonPositiveResourceType {
                actual: resource_type,
            });
        }
        if resource_types[..index].contains(&resource_type) {
            return Err(ListConfigResourcesRequestFailure::DuplicateResourceType {
                actual: resource_type,
            });
        }
    }
    Ok(())
}
