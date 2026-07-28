//! Tracked coordinator submission policy for generated `ConsumerGroupDescribe`.

use std::{error::Error, fmt};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse};

use crate::clock::OperationDeadline;

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const CONSUMER_GROUP_DESCRIBE_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const CONSUMER_GROUP_DESCRIBE_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum ConsumerGroupDescribeSubmitError {
    GroupMismatch,
    InvalidGroupBatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for ConsumerGroupDescribeSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                formatter.write_str("ConsumerGroupDescribe route and request groups differ")
            }
            Self::InvalidGroupBatch => {
                formatter.write_str("ConsumerGroupDescribe request is not a singleton")
            }
            Self::InvalidGroup(source) => {
                write!(
                    formatter,
                    "invalid ConsumerGroupDescribe coordinator key: {source}"
                )
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected ConsumerGroupDescribe: {source}")
            }
        }
    }
}

impl Error for ConsumerGroupDescribeSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::GroupMismatch | Self::InvalidGroupBatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_consumer_group_describe(
        &self,
        group_id: &str,
        request: ConsumerGroupDescribeRequest,
        deadline: OperationDeadline,
    ) -> Result<RoutedCall<ConsumerGroupDescribeResponse>, ConsumerGroupDescribeSubmitError> {
        let route = consumer_group_describe_route(group_id, &request)?;
        self.driver
            .request_tracked_with(route, request, consumer_group_describe_options(deadline))
            .map_err(ConsumerGroupDescribeSubmitError::Driver)
    }
}

pub(super) fn consumer_group_describe_route(
    group_id: &str,
    request: &ConsumerGroupDescribeRequest,
) -> Result<Route, ConsumerGroupDescribeSubmitError> {
    let [requested] = request.group_ids.as_slice() else {
        return Err(ConsumerGroupDescribeSubmitError::InvalidGroupBatch);
    };
    if requested.as_str() != group_id {
        return Err(ConsumerGroupDescribeSubmitError::GroupMismatch);
    }
    group_coordinator_route(group_id).map_err(ConsumerGroupDescribeSubmitError::InvalidGroup)
}

pub(super) const fn consumer_group_describe_options(deadline: OperationDeadline) -> RequestOptions {
    RequestOptions::new(deadline.transport())
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(CONSUMER_GROUP_DESCRIBE_MIN_VERSION)
        .with_maximum_version(CONSUMER_GROUP_DESCRIBE_MAX_VERSION)
}
