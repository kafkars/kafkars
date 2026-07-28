//! Tracked leader submission policy for Admin `DeleteRecords`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{DeleteRecordsRequest, DeleteRecordsResponse};

use super::super::DriverOwner;

const DELETE_RECORDS_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DELETE_RECORDS_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum DeleteRecordsSubmitError {
    InvalidTopic(TopicNameError),
    InvalidPartition(PartitionIdError),
    Driver(SubmitError),
}

impl fmt::Display for DeleteRecordsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => {
                write!(formatter, "invalid Admin DeleteRecords topic: {source}")
            }
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid Admin DeleteRecords partition: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected Admin DeleteRecords: {source}")
            }
        }
    }
}

impl Error for DeleteRecordsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one destructive operation against the current partition leader.
    pub(crate) fn submit_tracked_delete_records(
        &self,
        topic: &str,
        partition: i32,
        request: DeleteRecordsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DeleteRecordsResponse>, DeleteRecordsSubmitError> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(DeleteRecordsSubmitError::InvalidTopic)?;
        let partition =
            PartitionId::new(partition).map_err(DeleteRecordsSubmitError::InvalidPartition)?;
        self.driver
            .request_tracked_with(
                Route::PartitionLeader { topic, partition },
                request,
                delete_records_options(deadline),
            )
            .map_err(DeleteRecordsSubmitError::Driver)
    }
}

pub(super) const fn delete_records_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DELETE_RECORDS_MIN_VERSION)
        .with_maximum_version(DELETE_RECORDS_MAX_VERSION)
}
