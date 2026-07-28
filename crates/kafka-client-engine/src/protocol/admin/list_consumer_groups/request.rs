//! Version-neutral unfiltered `ListGroups` request materialization.

use kafka_wire::ListGroupsRequest;

/// Builds an empty-filter request representable across `ListGroups` `v0-v5`.
pub(crate) fn list_consumer_groups_request() -> ListGroupsRequest {
    let mut request = ListGroupsRequest::default();
    request.states_filter = Vec::new();
    request.types_filter = Vec::new();
    request
}
