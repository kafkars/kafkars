//! Concrete coordinator-routed submission of one generated `SyncGroup` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{SyncGroupRequest, SyncGroupResponse};

use crate::protocol::consumer::{CLASSIC_SYNC_MAX_VERSION, CLASSIC_SYNC_MIN_VERSION};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

// v2 is the last version before group_instance_id introduces static-membership
// semantics that deterministic membership policy deliberately defers.
pub(super) const SYNC_GROUP_MIN_VERSION: ApiVersion = ApiVersion::new(CLASSIC_SYNC_MIN_VERSION);
pub(super) const SYNC_GROUP_MAX_VERSION: ApiVersion = ApiVersion::new(CLASSIC_SYNC_MAX_VERSION);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum SyncGroupSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for SyncGroupSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid SyncGroup coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected SyncGroup: {source}"),
        }
    }
}

impl Error for SyncGroupSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_sync_group(
        &self,
        group: &str,
        request: SyncGroupRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<SyncGroupResponse>, SyncGroupSubmitError> {
        let route = group_coordinator_route(group).map_err(SyncGroupSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(route, request, sync_group_options(deadline))
            .map_err(SyncGroupSubmitError::Driver)
    }
}

pub(super) const fn sync_group_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(SYNC_GROUP_MIN_VERSION)
        .with_maximum_version(SYNC_GROUP_MAX_VERSION)
}
