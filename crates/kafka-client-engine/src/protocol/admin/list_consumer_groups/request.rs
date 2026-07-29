//! Bounded API 16 request materialization with exact filter version floors.

use core::mem::size_of;

use kafka_client_core::AdminGroupListingFilters;
use kafka_wire::{ListGroupsRequest, RetainedSize};
use kafka_wire_core::StrBytes;

/// Pre-driver request materialization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupsRequestFailure {
    RetainedBytes,
    Allocation,
}

/// Builds exact broker-side filters and their required API 16 version floor.
pub(crate) fn list_consumer_groups_request(
    filters: &AdminGroupListingFilters,
    retained_limit: usize,
) -> Result<(ListGroupsRequest, i16), ListConsumerGroupsRequestFailure> {
    let required =
        request_charge(filters).ok_or(ListConsumerGroupsRequestFailure::RetainedBytes)?;
    if required > retained_limit {
        return Err(ListConsumerGroupsRequestFailure::RetainedBytes);
    }
    let mut request = ListGroupsRequest::default();
    request.states_filter = copy_filters(filters.state_filters())?;
    request.types_filter = copy_filters(filters.group_type_filters())?;
    if request.retained_size().heap_bytes() > retained_limit {
        return Err(ListConsumerGroupsRequestFailure::RetainedBytes);
    }
    Ok((request, filters.minimum_list_groups_version()))
}

fn copy_filters(filters: &[String]) -> Result<Vec<StrBytes>, ListConsumerGroupsRequestFailure> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(filters.len())
        .map_err(|_| ListConsumerGroupsRequestFailure::Allocation)?;
    copied.extend(filters.iter().map(|filter| filter.as_str().into()));
    Ok(copied)
}

fn request_charge(filters: &AdminGroupListingFilters) -> Option<usize> {
    let count = filters
        .state_filters()
        .len()
        .checked_add(filters.group_type_filters().len())?;
    let text = filters
        .state_filters()
        .iter()
        .chain(filters.group_type_filters())
        .try_fold(0usize, |bytes, filter| bytes.checked_add(filter.len()))?;
    size_of::<ListGroupsRequest>()
        .checked_add(count.checked_mul(size_of::<StrBytes>())?)?
        .checked_add(text)
}
