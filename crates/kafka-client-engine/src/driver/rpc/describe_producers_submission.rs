//! Tracked leader or exact-broker submission policy for Admin `DescribeProducers`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall,
    SubmitError, TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{DescribeProducersRequest, DescribeProducersResponse};

use super::super::DriverOwner;

const DESCRIBE_PRODUCERS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum DescribeProducersSubmitError {
    InvalidBroker(InvalidBroker),
    InvalidTopic(TopicNameError),
    InvalidPartition(PartitionIdError),
    Driver(SubmitError),
}

impl fmt::Display for DescribeProducersSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(formatter, "invalid DescribeProducers broker: {source}")
            }
            Self::InvalidTopic(source) => {
                write!(formatter, "invalid DescribeProducers topic: {source}")
            }
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid DescribeProducers partition: {source}")
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected DescribeProducers: {source}")
            }
        }
    }
}

impl Error for DescribeProducersSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one v0 query to the selected exact broker or current leader.
    pub(crate) fn submit_tracked_describe_producers(
        &self,
        topic: &str,
        partition: i32,
        broker_id: Option<i32>,
        request: DescribeProducersRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeProducersResponse>, DescribeProducersSubmitError> {
        let route = describe_producers_route(topic, partition, broker_id)?;
        self.driver
            .request_tracked_with(route, request, describe_producers_options(deadline))
            .map_err(DescribeProducersSubmitError::Driver)
    }
}

pub(super) fn describe_producers_route(
    topic: &str,
    partition: i32,
    broker_id: Option<i32>,
) -> Result<Route, DescribeProducersSubmitError> {
    let topic =
        TopicName::new(topic.to_owned()).map_err(DescribeProducersSubmitError::InvalidTopic)?;
    let partition =
        PartitionId::new(partition).map_err(DescribeProducersSubmitError::InvalidPartition)?;
    if let Some(broker_id) = broker_id {
        let broker_id = BrokerId::new(broker_id).map_err(|_error| {
            DescribeProducersSubmitError::InvalidBroker(InvalidBroker(broker_id))
        })?;
        return Ok(Route::Broker { broker_id });
    }
    Ok(Route::PartitionLeader { topic, partition })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidBroker(i32);

impl fmt::Display for InvalidBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.0)
    }
}

impl Error for InvalidBroker {}

pub(super) const fn describe_producers_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_PRODUCERS_VERSION)
        .with_maximum_version(DESCRIBE_PRODUCERS_VERSION)
}
