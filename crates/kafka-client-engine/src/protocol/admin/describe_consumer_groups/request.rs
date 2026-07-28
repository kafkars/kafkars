//! Singleton generated `DescribeGroups` request construction.

use kafka_wire::DescribeGroupsRequest;

/// Builds one exact group query for coordinator routing.
pub(crate) fn describe_consumer_group_request(
    group_id: &str,
    include_authorized_operations: bool,
) -> DescribeGroupsRequest {
    let mut request = DescribeGroupsRequest::default();
    request.groups = vec![group_id.into()];
    request.include_authorized_operations = include_authorized_operations;
    request
}
