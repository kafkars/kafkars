//! Concrete coordinator-routed submission of one generated `JoinGroup` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{JoinGroupRequest, JoinGroupResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

// v1 carries the separate rebalance timeout. v3 is the last version before the
// MEMBER_ID_REQUIRED handshake that deterministic membership policy does not
// yet own.
pub(super) const JOIN_GROUP_MIN_VERSION: ApiVersion = ApiVersion::new(1);
pub(super) const JOIN_GROUP_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum JoinGroupSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for JoinGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid JoinGroup coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected JoinGroup: {source}"),
        }
    }
}

impl Error for JoinGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_join_group(
        &self,
        group: &str,
        request: JoinGroupRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<JoinGroupResponse>, JoinGroupSubmitError> {
        let route = group_coordinator_route(group).map_err(JoinGroupSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(route, request, join_group_options(deadline))
            .map_err(JoinGroupSubmitError::Driver)
    }
}

pub(super) const fn join_group_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(JOIN_GROUP_MIN_VERSION)
        .with_maximum_version(JOIN_GROUP_MAX_VERSION)
}
