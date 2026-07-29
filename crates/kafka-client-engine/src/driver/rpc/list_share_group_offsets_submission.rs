//! Tracked group-coordinator submission of generated API-key 90 v0-v1 requests.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const LIST_SHARE_GROUP_OFFSETS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const LIST_SHARE_GROUP_OFFSETS_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent failure before driver ownership of the read-only request.
#[derive(Debug)]
pub(crate) enum ListShareGroupOffsetsSubmitError {
    GroupCount { actual: usize },
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for ListShareGroupOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupCount { actual } => {
                write!(
                    formatter,
                    "API-90 request contains {actual} groups instead of one"
                )
            }
            Self::GroupMismatch => {
                formatter.write_str("API-90 route and request group identities differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid share-group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected DescribeShareGroupOffsets: {source}"
                )
            }
        }
    }
}

impl Error for ListShareGroupOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::GroupCount { .. } | Self::GroupMismatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_list_share_group_offsets(
        &self,
        group: &str,
        request: DescribeShareGroupOffsetsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeShareGroupOffsetsResponse>, ListShareGroupOffsetsSubmitError>
    {
        let route = list_share_group_offsets_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, list_share_group_offsets_options(deadline))
            .map_err(ListShareGroupOffsetsSubmitError::Driver)
    }
}

pub(super) fn list_share_group_offsets_route(
    group: &str,
    request: &DescribeShareGroupOffsetsRequest,
) -> Result<Route, ListShareGroupOffsetsSubmitError> {
    let [requested] = request.groups.as_slice() else {
        return Err(ListShareGroupOffsetsSubmitError::GroupCount {
            actual: request.groups.len(),
        });
    };
    if requested.group_id.as_str() != group {
        return Err(ListShareGroupOffsetsSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(ListShareGroupOffsetsSubmitError::InvalidGroup)
}

pub(super) const fn list_share_group_offsets_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LIST_SHARE_GROUP_OFFSETS_MIN_VERSION)
        .with_maximum_version(LIST_SHARE_GROUP_OFFSETS_MAX_VERSION)
}
