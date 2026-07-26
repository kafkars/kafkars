//! Coordinator-routed tracked submission of assigned-partition `OffsetFetch`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::OffsetFetchResponse;

use crate::protocol::consumer::GroupOffsetFetchRequest;

use super::super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const GROUP_POSITION_OFFSET_FETCH_MIN_VERSION: ApiVersion = ApiVersion::new(2);
const GROUP_POSITION_OFFSET_FETCH_MAX_VERSION: ApiVersion = ApiVersion::new(9);

/// Definitely-unsent failure before tracked driver ownership.
#[derive(Debug)]
pub(crate) enum GroupPositionOffsetFetchSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for GroupPositionOffsetFetchSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected group position OffsetFetch: {source}"
                )
            }
        }
    }
}

impl Error for GroupPositionOffsetFetchSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_group_position_offset_fetch(
        &self,
        group: &str,
        request: GroupOffsetFetchRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<OffsetFetchResponse>, GroupPositionOffsetFetchSubmitError> {
        let route = group_coordinator_route(group)
            .map_err(GroupPositionOffsetFetchSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                group_position_offset_fetch_options(deadline),
            )
            .map_err(GroupPositionOffsetFetchSubmitError::Driver)
    }
}

pub(super) const fn group_position_offset_fetch_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(GROUP_POSITION_OFFSET_FETCH_MIN_VERSION)
        .with_maximum_version(GROUP_POSITION_OFFSET_FETCH_MAX_VERSION)
}
