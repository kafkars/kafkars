//! Tracked partition-leader submission of one exact generated `Fetch` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall,
    SubmitError, TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{FetchRequest, FetchResponse};

use crate::{driver::DriverOwner, protocol::fetch::FETCH_NAME_ROUTE_MAX_VERSION};

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum FetchSubmitError {
    /// The engine supplied a topic outside the driver's validated domain.
    InvalidTopic(TopicNameError),
    /// The engine supplied a partition outside the driver's validated domain.
    InvalidPartition(PartitionIdError),
    /// Bounded driver admission rejected the request.
    Driver(SubmitError),
}

impl fmt::Display for FetchSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => write!(formatter, "invalid Fetch topic: {source}"),
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid Fetch partition: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected Fetch: {source}"),
        }
    }
}

impl Error for FetchSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one long-poll fetch against the driver's current partition leader.
    ///
    /// The returned call retains the selected API version and exact route
    /// authority while the consumer owner retains its fetch fence.
    pub(crate) fn submit_tracked_fetch(
        &self,
        topic: &str,
        partition: i32,
        request: FetchRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<FetchResponse>, FetchSubmitError> {
        let topic = TopicName::new(topic.to_owned()).map_err(FetchSubmitError::InvalidTopic)?;
        let partition = PartitionId::new(partition).map_err(FetchSubmitError::InvalidPartition)?;
        let options = fetch_options_for_request(deadline, &request);
        self.driver
            .request_tracked_with(
                Route::PartitionLeader { topic, partition },
                request,
                options,
            )
            .map_err(FetchSubmitError::Driver)
    }

    /// Submits one broker-aggregated long-poll Fetch under exact metadata authority.
    pub(crate) fn submit_tracked_broker_fetch(
        &self,
        broker_id: BrokerId,
        request: FetchRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<FetchResponse>, FetchSubmitError> {
        let options = fetch_options_for_request(deadline, &request);
        self.driver
            .request_tracked_with(Route::Broker { broker_id }, request, options)
            .map_err(FetchSubmitError::Driver)
    }
}

pub(super) const fn fetch_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::LongPoll)
        .with_maximum_version(ApiVersion::new(FETCH_NAME_ROUTE_MAX_VERSION))
}

pub(super) const fn fetch_options_for_request(
    deadline: Instant,
    request: &FetchRequest,
) -> RequestOptions {
    let options = fetch_options(deadline);
    if request.session_id > 0 {
        options.with_minimum_version(ApiVersion::new(7))
    } else {
        options
    }
}
