//! Tracked group-coordinator submission of one all-topic `OffsetFetch`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::OffsetFetchResponse;

use crate::protocol::admin::group_offsets::GroupOffsetsRequest;

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const GROUP_OFFSETS_MIN_VERSION: ApiVersion = ApiVersion::new(2);
const GROUP_OFFSETS_STABLE_MIN_VERSION: ApiVersion = ApiVersion::new(7);
const GROUP_OFFSETS_MAX_VERSION: ApiVersion = ApiVersion::new(9);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) enum GroupOffsetsSubmitError {
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for GroupOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid group coordinator key: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected OffsetFetch: {source}"),
        }
    }
}

impl Error for GroupOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_group_offsets(
        &self,
        group: &str,
        request: GroupOffsetsRequest,
        deadline: Instant,
        require_stable: bool,
    ) -> Result<RoutedCall<OffsetFetchResponse>, GroupOffsetsSubmitError> {
        let route =
            group_coordinator_route(group).map_err(GroupOffsetsSubmitError::InvalidGroup)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                group_offsets_options(deadline, require_stable),
            )
            .map_err(GroupOffsetsSubmitError::Driver)
    }
}

pub(super) const fn group_offsets_options(
    deadline: Instant,
    require_stable: bool,
) -> RequestOptions {
    let options = RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(GROUP_OFFSETS_MAX_VERSION);
    if require_stable {
        options.with_minimum_version(GROUP_OFFSETS_STABLE_MIN_VERSION)
    } else {
        options.with_minimum_version(GROUP_OFFSETS_MIN_VERSION)
    }
}
