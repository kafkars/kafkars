//! Bounded singleton Admin `DeleteConsumerGroups` request construction.

use core::mem::size_of;

use kafka_client_core::DeleteConsumerGroupsTarget;
use kafka_wire::{DeleteGroupsRequest, RetainedSize};
use kafka_wire_core::StrBytes;

/// Insufficient retained capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteConsumerGroupsRequestFailure;

/// Builds one destructive request for one caller group.
pub(crate) fn delete_consumer_groups_request(
    target: &DeleteConsumerGroupsTarget,
    retained_limit: usize,
) -> Result<DeleteGroupsRequest, DeleteConsumerGroupsRequestFailure> {
    let required = delete_consumer_groups_request_peak_charge(target)
        .ok_or(DeleteConsumerGroupsRequestFailure)?;
    if required > retained_limit {
        return Err(DeleteConsumerGroupsRequestFailure);
    }
    let mut group_id = String::new();
    group_id
        .try_reserve_exact(target.group_id().len())
        .map_err(|_| DeleteConsumerGroupsRequestFailure)?;
    group_id.push_str(target.group_id());
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(1)
        .map_err(|_| DeleteConsumerGroupsRequestFailure)?;
    groups.push(group_id.into());
    let mut request = DeleteGroupsRequest::default();
    request.groups_names = groups;
    if request.retained_size().heap_bytes() > retained_limit {
        return Err(DeleteConsumerGroupsRequestFailure);
    }
    Ok(request)
}

/// Returns the checked peak allocation for one generated singleton request.
pub(crate) fn delete_consumer_groups_request_peak_charge(
    target: &DeleteConsumerGroupsTarget,
) -> Option<usize> {
    size_of::<DeleteGroupsRequest>()
        .checked_add(size_of::<StrBytes>())?
        .checked_add(target.group_id().len())
}
