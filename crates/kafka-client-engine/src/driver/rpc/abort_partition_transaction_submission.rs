//! Tracked leader submission policy for API27 partition transaction aborts.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{WriteTxnMarkersRequest, WriteTxnMarkersResponse};

use super::super::DriverOwner;

const ABORT_PARTITION_TRANSACTION_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const ABORT_PARTITION_TRANSACTION_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum AbortPartitionTransactionSubmitError {
    InvalidTopic(TopicNameError),
    InvalidPartition(PartitionIdError),
    Driver(SubmitError),
}

impl fmt::Display for AbortPartitionTransactionSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => {
                write!(
                    formatter,
                    "invalid partition transaction-abort topic: {source}"
                )
            }
            Self::InvalidPartition(source) => {
                write!(
                    formatter,
                    "invalid partition transaction-abort partition: {source}"
                )
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected partition transaction abort: {source}"
                )
            }
        }
    }
}

impl Error for AbortPartitionTransactionSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one abort marker against the current partition leader.
    pub(crate) fn submit_tracked_abort_partition_transaction(
        &self,
        topic: &str,
        partition: i32,
        request: WriteTxnMarkersRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<WriteTxnMarkersResponse>, AbortPartitionTransactionSubmitError> {
        let topic = TopicName::new(topic.to_owned())
            .map_err(AbortPartitionTransactionSubmitError::InvalidTopic)?;
        let partition = PartitionId::new(partition)
            .map_err(AbortPartitionTransactionSubmitError::InvalidPartition)?;
        self.driver
            .request_tracked_with(
                Route::PartitionLeader { topic, partition },
                request,
                abort_partition_transaction_options(deadline),
            )
            .map_err(AbortPartitionTransactionSubmitError::Driver)
    }
}

pub(super) const fn abort_partition_transaction_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ABORT_PARTITION_TRANSACTION_MIN_VERSION)
        .with_maximum_version(ABORT_PARTITION_TRANSACTION_MAX_VERSION)
}
