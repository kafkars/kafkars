//! Bounded singleton `ConsumerGroupDescribe` request construction.

use core::mem::size_of;

use kafka_wire::ConsumerGroupDescribeRequest;

/// Definitely-unsent request-shape or retained-capacity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerGroupDescribeRequestFailure {
    EmptyGroupId,
    GroupIdTooLong,
    RetainedBytes,
}

/// Builds one coordinator-correlated API-key 69 request.
pub(crate) fn consumer_group_describe_request(
    group_id: &str,
    include_authorized_operations: bool,
    retained_limit: usize,
) -> Result<ConsumerGroupDescribeRequest, ConsumerGroupDescribeRequestFailure> {
    if group_id.is_empty() {
        return Err(ConsumerGroupDescribeRequestFailure::EmptyGroupId);
    }
    if group_id.len() > i16::MAX as usize {
        return Err(ConsumerGroupDescribeRequestFailure::GroupIdTooLong);
    }
    let retained_bytes = size_of::<ConsumerGroupDescribeRequest>()
        .checked_add(size_of::<kafka_wire_core::StrBytes>())
        .and_then(|bytes| bytes.checked_add(group_id.len()))
        .ok_or(ConsumerGroupDescribeRequestFailure::RetainedBytes)?;
    if retained_bytes > retained_limit {
        return Err(ConsumerGroupDescribeRequestFailure::RetainedBytes);
    }
    let mut request = ConsumerGroupDescribeRequest::default();
    request.group_ids = vec![group_id.into()];
    request.include_authorized_operations = include_authorized_operations;
    Ok(request)
}
