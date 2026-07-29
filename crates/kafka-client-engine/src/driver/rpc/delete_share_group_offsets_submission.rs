//! Exact-v0 group-coordinator submission policy for API key 92.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, CoordinatorKeyError, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse};

use super::{super::DriverOwner, group_coordinator_route::group_coordinator_route};

const DELETE_SHARE_GROUP_OFFSETS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent rejection before API-92 request ownership.
#[derive(Debug)]
pub(crate) enum DeleteShareGroupOffsetsSubmitError {
    GroupMismatch,
    InvalidGroup(CoordinatorKeyError),
    Driver(SubmitError),
}

impl fmt::Display for DeleteShareGroupOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMismatch => {
                formatter.write_str("API-92 route and request group identities differ")
            }
            Self::InvalidGroup(source) => {
                write!(formatter, "invalid share-group coordinator key: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected DeleteShareGroupOffsets: {source}"
                )
            }
        }
    }
}

impl Error for DeleteShareGroupOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidGroup(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::GroupMismatch => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_delete_share_group_offsets(
        &self,
        group: &str,
        request: DeleteShareGroupOffsetsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DeleteShareGroupOffsetsResponse>, DeleteShareGroupOffsetsSubmitError>
    {
        let route = delete_share_group_offsets_route(group, &request)?;
        self.driver
            .request_tracked_with(route, request, delete_share_group_offsets_options(deadline))
            .map_err(DeleteShareGroupOffsetsSubmitError::Driver)
    }
}

pub(super) fn delete_share_group_offsets_route(
    group: &str,
    request: &DeleteShareGroupOffsetsRequest,
) -> Result<Route, DeleteShareGroupOffsetsSubmitError> {
    if request.group_id.as_str() != group {
        return Err(DeleteShareGroupOffsetsSubmitError::GroupMismatch);
    }
    group_coordinator_route(group).map_err(DeleteShareGroupOffsetsSubmitError::InvalidGroup)
}

pub(super) const fn delete_share_group_offsets_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DELETE_SHARE_GROUP_OFFSETS_VERSION)
        .with_maximum_version(DELETE_SHARE_GROUP_OFFSETS_VERSION)
}
