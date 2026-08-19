//! Tracked `AnyBroker` submission policy for Admin `DescribeTopicPartitions`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse};

use super::super::DriverOwner;

const DESCRIBE_TOPIC_PARTITIONS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) struct DescribeTopicPartitionsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeTopicPartitionsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeTopicPartitions request: {}",
            self.source
        )
    }
}

impl Error for DescribeTopicPartitionsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one v0 topic-partition description through any broker.
    pub(crate) fn submit_tracked_describe_topic_partitions(
        &self,
        request: DescribeTopicPartitionsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeTopicPartitionsResponse>, DescribeTopicPartitionsSubmitError>
    {
        self.driver
            .request_tracked_with(
                describe_topic_partitions_route(),
                request,
                describe_topic_partitions_options(deadline),
            )
            .map_err(|source| DescribeTopicPartitionsSubmitError { source })
    }
}

pub(super) const fn describe_topic_partitions_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_topic_partitions_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_TOPIC_PARTITIONS_VERSION)
        .with_maximum_version(DESCRIBE_TOPIC_PARTITIONS_VERSION)
}
