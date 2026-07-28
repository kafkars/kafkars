//! Tracked group-coordinator submission policy for generated `DescribeGroups`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{DescribeGroupsRequest, DescribeGroupsResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const DESCRIBE_GROUPS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DESCRIBE_GROUPS_AUTH_MIN_VERSION: ApiVersion = ApiVersion::new(3);
const DESCRIBE_GROUPS_MAX_VERSION: ApiVersion = ApiVersion::new(6);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum DescribeConsumerGroupsSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeConsumerGroupsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(
                    formatter,
                    "invalid DescribeGroups coordinator key: {source}"
                )
            }
            Self::Driver(source) => write!(formatter, "driver rejected DescribeGroups: {source}"),
        }
    }
}

impl Error for DescribeConsumerGroupsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_describe_consumer_group(
        &self,
        group_id: &str,
        request: DescribeGroupsRequest,
        deadline: Instant,
        include_authorized_operations: bool,
    ) -> Result<RoutedCall<DescribeGroupsResponse>, DescribeConsumerGroupsSubmitError> {
        let route = group_coordinator_route(group_id)
            .map_err(DescribeConsumerGroupsSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                describe_consumer_groups_options(deadline, include_authorized_operations),
            )
            .map_err(DescribeConsumerGroupsSubmitError::Driver)
    }
}

pub(super) const fn describe_consumer_groups_options(
    deadline: Instant,
    include_authorized_operations: bool,
) -> RequestOptions {
    let minimum = if include_authorized_operations {
        DESCRIBE_GROUPS_AUTH_MIN_VERSION
    } else {
        DESCRIBE_GROUPS_MIN_VERSION
    };
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(minimum)
        .with_maximum_version(DESCRIBE_GROUPS_MAX_VERSION)
}
