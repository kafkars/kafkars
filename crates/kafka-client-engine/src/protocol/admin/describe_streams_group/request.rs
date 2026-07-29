//! Bounded singleton API-key 89 request construction for stable versions 0 and 1.

use core::mem::size_of;

use kafka_wire::StreamsGroupDescribeRequest;

/// Definitely-unsent request-shape, allocation, or retained-capacity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeStreamsGroupRequestFailure {
    EmptyGroupId,
    GroupIdTooLong,
    RetainedBytes,
    Allocation,
}

/// Builds one coordinator-correlated API-key 89 request.
pub(crate) fn describe_streams_group_request(
    group_id: &str,
    include_authorized_operations: bool,
    include_topology_description: bool,
    retained_limit: usize,
) -> Result<StreamsGroupDescribeRequest, DescribeStreamsGroupRequestFailure> {
    if group_id.is_empty() {
        return Err(DescribeStreamsGroupRequestFailure::EmptyGroupId);
    }
    if group_id.len() > i16::MAX as usize {
        return Err(DescribeStreamsGroupRequestFailure::GroupIdTooLong);
    }
    let retained_bytes = size_of::<StreamsGroupDescribeRequest>()
        .checked_add(size_of::<kafka_wire_core::StrBytes>())
        .and_then(|bytes| bytes.checked_add(group_id.len()))
        .ok_or(DescribeStreamsGroupRequestFailure::RetainedBytes)?;
    if retained_bytes > retained_limit {
        return Err(DescribeStreamsGroupRequestFailure::RetainedBytes);
    }
    let mut group_ids = Vec::new();
    group_ids
        .try_reserve_exact(1)
        .map_err(|_| DescribeStreamsGroupRequestFailure::Allocation)?;
    group_ids.push(group_id.into());
    let mut request = StreamsGroupDescribeRequest::default();
    request.group_ids = group_ids;
    request.include_authorized_operations = include_authorized_operations;
    request.include_topology_description = include_topology_description;
    Ok(request)
}
