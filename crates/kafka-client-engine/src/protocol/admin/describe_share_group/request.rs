//! Bounded singleton exact-v1 `ShareGroupDescribe` request construction.

use core::mem::size_of;

use kafka_wire::ShareGroupDescribeRequest;

/// Definitely-unsent request-shape, allocation, or retained-capacity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupRequestFailure {
    EmptyGroupId,
    GroupIdTooLong,
    RetainedBytes,
    Allocation,
}

/// Builds one coordinator-correlated API-key 77 request.
pub(crate) fn describe_share_group_request(
    group_id: &str,
    include_authorized_operations: bool,
    retained_limit: usize,
) -> Result<ShareGroupDescribeRequest, DescribeShareGroupRequestFailure> {
    if group_id.is_empty() {
        return Err(DescribeShareGroupRequestFailure::EmptyGroupId);
    }
    if group_id.len() > i16::MAX as usize {
        return Err(DescribeShareGroupRequestFailure::GroupIdTooLong);
    }
    let retained_bytes = size_of::<ShareGroupDescribeRequest>()
        .checked_add(size_of::<kafka_wire_core::StrBytes>())
        .and_then(|bytes| bytes.checked_add(group_id.len()))
        .ok_or(DescribeShareGroupRequestFailure::RetainedBytes)?;
    if retained_bytes > retained_limit {
        return Err(DescribeShareGroupRequestFailure::RetainedBytes);
    }
    let mut group_ids = Vec::new();
    group_ids
        .try_reserve_exact(1)
        .map_err(|_| DescribeShareGroupRequestFailure::Allocation)?;
    group_ids.push(group_id.into());
    let mut request = ShareGroupDescribeRequest::default();
    request.group_ids = group_ids;
    request.include_authorized_operations = include_authorized_operations;
    Ok(request)
}
