//! Tracked coordinator submission policy for Admin `DeleteConsumerGroups`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{DeleteGroupsRequest, DeleteGroupsResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const DELETE_CONSUMER_GROUPS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DELETE_CONSUMER_GROUPS_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum DeleteConsumerGroupsSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DeleteConsumerGroupsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(
                    formatter,
                    "invalid Admin DeleteConsumerGroups group: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected Admin DeleteConsumerGroups: {source}"
                )
            }
        }
    }
}

impl Error for DeleteConsumerGroupsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one destructive operation against the current group coordinator.
    pub(crate) fn submit_tracked_delete_consumer_groups(
        &self,
        group_id: &str,
        request: DeleteGroupsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DeleteGroupsResponse>, DeleteConsumerGroupsSubmitError> {
        let route = group_coordinator_route(group_id)
            .map_err(DeleteConsumerGroupsSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(route, request, delete_consumer_groups_options(deadline))
            .map_err(DeleteConsumerGroupsSubmitError::Driver)
    }
}

pub(super) const fn delete_consumer_groups_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DELETE_CONSUMER_GROUPS_MIN_VERSION)
        .with_maximum_version(DELETE_CONSUMER_GROUPS_MAX_VERSION)
}
