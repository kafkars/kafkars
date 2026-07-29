//! Conditional-v0/v1 group-coordinator submission policy for API key 89.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{StreamsGroupDescribeRequest, StreamsGroupDescribeResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const DESCRIBE_STREAMS_GROUP_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DESCRIBE_STREAMS_GROUP_TOPOLOGY_VERSION: ApiVersion = ApiVersion::new(1);
const DESCRIBE_STREAMS_GROUP_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent rejection before API-89 request ownership.
#[derive(Debug)]
pub(crate) enum DescribeStreamsGroupSubmitError {
    InvalidGroupBatch,
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeStreamsGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroupBatch => {
                formatter.write_str("StreamsGroupDescribe request is not a singleton")
            }
            Self::GroupMismatch => {
                formatter.write_str("StreamsGroupDescribe route and request group differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid streams-group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected StreamsGroupDescribe: {source}")
            }
        }
    }
}

impl Error for DescribeStreamsGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::InvalidGroupBatch | Self::GroupMismatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_describe_streams_group(
        &self,
        group: &str,
        request: StreamsGroupDescribeRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<StreamsGroupDescribeResponse>, DescribeStreamsGroupSubmitError> {
        let route = describe_streams_group_route(group, &request)?;
        let include_topology_description = request.include_topology_description;
        self.driver
            .request_tracked_with(
                route,
                request,
                describe_streams_group_options(deadline, include_topology_description),
            )
            .map_err(DescribeStreamsGroupSubmitError::Driver)
    }
}

pub(super) fn describe_streams_group_route(
    group: &str,
    request: &StreamsGroupDescribeRequest,
) -> Result<Route, DescribeStreamsGroupSubmitError> {
    let [requested] = request.group_ids.as_slice() else {
        return Err(DescribeStreamsGroupSubmitError::InvalidGroupBatch);
    };
    if requested.as_str() != group {
        return Err(DescribeStreamsGroupSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(DescribeStreamsGroupSubmitError::InvalidGroup)
}

pub(super) const fn describe_streams_group_options(
    deadline: Instant,
    include_topology_description: bool,
) -> RequestOptions {
    let minimum = if include_topology_description {
        DESCRIBE_STREAMS_GROUP_TOPOLOGY_VERSION
    } else {
        DESCRIBE_STREAMS_GROUP_MIN_VERSION
    };
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(minimum)
        .with_maximum_version(DESCRIBE_STREAMS_GROUP_MAX_VERSION)
}
