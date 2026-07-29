//! Exact empty request construction for flexible API-key 74 v0.

use kafka_wire::ListConfigResourcesRequest;

/// Builds the sole v0 request, whose empty type list selects client-metrics resources.
pub(crate) fn list_client_metrics_resources_request() -> ListConfigResourcesRequest {
    ListConfigResourcesRequest::default()
}
