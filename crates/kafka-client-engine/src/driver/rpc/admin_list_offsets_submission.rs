//! Tracked leader submission policy for Admin `ListOffsets`.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::{AdminListOffsetTarget, ReadIsolation};
use kafka_driver::{
    ApiVersion, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{ListOffsetsRequest, ListOffsetsResponse};

use crate::protocol::admin::list_offsets::minimum_api_version;

use super::super::DriverOwner;

const ADMIN_LIST_OFFSETS_MAX_VERSION: ApiVersion = ApiVersion::new(11);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum AdminListOffsetsSubmitError {
    InvalidTopic(TopicNameError),
    InvalidPartition(PartitionIdError),
    Driver(SubmitError),
}

impl fmt::Display for AdminListOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => {
                write!(formatter, "invalid Admin ListOffsets topic: {source}")
            }
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid Admin ListOffsets partition: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected Admin ListOffsets: {source}")
            }
        }
    }
}

impl Error for AdminListOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one admin query against the driver's current partition leader.
    pub(crate) fn submit_tracked_admin_list_offsets(
        &self,
        target: &AdminListOffsetTarget,
        read_isolation: ReadIsolation,
        request: ListOffsetsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ListOffsetsResponse>, AdminListOffsetsSubmitError> {
        let topic = TopicName::new(target.topic().to_owned())
            .map_err(AdminListOffsetsSubmitError::InvalidTopic)?;
        let partition = PartitionId::new(target.partition())
            .map_err(AdminListOffsetsSubmitError::InvalidPartition)?;
        self.driver
            .request_tracked_with(
                Route::PartitionLeader { topic, partition },
                request,
                admin_list_offsets_options(target, read_isolation, deadline),
            )
            .map_err(AdminListOffsetsSubmitError::Driver)
    }
}

pub(super) const fn admin_list_offsets_options(
    target: &AdminListOffsetTarget,
    read_isolation: ReadIsolation,
    deadline: Instant,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(minimum_api_version(target, read_isolation)))
        .with_maximum_version(ADMIN_LIST_OFFSETS_MAX_VERSION)
}
