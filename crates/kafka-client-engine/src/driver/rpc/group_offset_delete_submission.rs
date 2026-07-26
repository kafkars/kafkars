//! Tracked group-coordinator submission of one generated `OffsetDelete` v0 request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{OffsetDeleteRequest, OffsetDeleteResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const GROUP_OFFSET_DELETE_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum GroupOffsetDeleteSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for GroupOffsetDeleteSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected OffsetDelete: {source}"),
        }
    }
}

impl Error for GroupOffsetDeleteSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_group_offset_delete(
        &self,
        group: &str,
        request: OffsetDeleteRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<OffsetDeleteResponse>, GroupOffsetDeleteSubmitError> {
        let route =
            group_coordinator_route(group).map_err(GroupOffsetDeleteSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(route, request, group_offset_delete_options(deadline))
            .map_err(GroupOffsetDeleteSubmitError::Driver)
    }
}

pub(super) const fn group_offset_delete_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(GROUP_OFFSET_DELETE_VERSION)
        .with_maximum_version(GROUP_OFFSET_DELETE_VERSION)
}
