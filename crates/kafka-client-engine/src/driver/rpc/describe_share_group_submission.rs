//! Exact-v1 group-coordinator submission policy for API key 77.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ShareGroupDescribeRequest, ShareGroupDescribeResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const DESCRIBE_SHARE_GROUP_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent rejection before API-77 request ownership.
#[derive(Debug)]
pub(crate) enum DescribeShareGroupSubmitError {
    InvalidGroupBatch,
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeShareGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroupBatch => {
                formatter.write_str("ShareGroupDescribe request is not a singleton")
            }
            Self::GroupMismatch => {
                formatter.write_str("ShareGroupDescribe route and request group differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid share-group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected ShareGroupDescribe: {source}")
            }
        }
    }
}

impl Error for DescribeShareGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::InvalidGroupBatch | Self::GroupMismatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_describe_share_group(
        &self,
        group: &str,
        request: ShareGroupDescribeRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ShareGroupDescribeResponse>, DescribeShareGroupSubmitError> {
        let route = describe_share_group_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, describe_share_group_options(deadline))
            .map_err(DescribeShareGroupSubmitError::Driver)
    }
}

pub(super) fn describe_share_group_route(
    group: &str,
    request: &ShareGroupDescribeRequest,
) -> Result<Route, DescribeShareGroupSubmitError> {
    let [requested] = request.group_ids.as_slice() else {
        return Err(DescribeShareGroupSubmitError::InvalidGroupBatch);
    };
    if requested.as_str() != group {
        return Err(DescribeShareGroupSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(DescribeShareGroupSubmitError::InvalidGroup)
}

pub(super) const fn describe_share_group_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_SHARE_GROUP_VERSION)
        .with_maximum_version(DESCRIBE_SHARE_GROUP_VERSION)
}
