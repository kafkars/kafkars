//! Tracked partition-leader submission of one generated `ListOffsets` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{ListOffsetsRequest, ListOffsetsResponse};

use super::super::DriverOwner;

const LIST_OFFSETS_MAX_VERSION: ApiVersion = ApiVersion::new(11);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum ListOffsetsSubmitError {
    /// The engine supplied a topic outside the driver's validated domain.
    InvalidTopic(TopicNameError),
    /// The engine supplied a partition outside the driver's validated domain.
    InvalidPartition(PartitionIdError),
    /// Bounded driver admission rejected the request.
    Driver(SubmitError),
}

impl fmt::Display for ListOffsetsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => write!(formatter, "invalid ListOffsets topic: {source}"),
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid ListOffsets partition: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected ListOffsets: {source}"),
        }
    }
}

impl Error for ListOffsetsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one position lookup against the driver's current partition leader.
    ///
    /// The caller retains the original absolute deadline and the returned
    /// routed call retains exact invalidation authority until core settles the
    /// fenced position attempt.
    pub(crate) fn submit_tracked_list_offsets(
        &self,
        topic: &str,
        partition: i32,
        request: ListOffsetsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ListOffsetsResponse>, ListOffsetsSubmitError> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(ListOffsetsSubmitError::InvalidTopic)?;
        let partition =
            PartitionId::new(partition).map_err(ListOffsetsSubmitError::InvalidPartition)?;
        self.driver
            .request_tracked_with(
                Route::PartitionLeader { topic, partition },
                request,
                list_offsets_options(deadline),
            )
            .map_err(ListOffsetsSubmitError::Driver)
    }
}

pub(super) const fn list_offsets_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(LIST_OFFSETS_MAX_VERSION)
}
