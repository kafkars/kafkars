//! Concrete tracked submission of one name-routed generated Produce request.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};
use kafka_driver::{
    ApiVersion, PartitionId, PartitionIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TopicName, TopicNameError, TrafficClass,
};
use kafka_wire::{ProduceRequest, ProduceResponse};

use super::super::DriverOwner;

const PRODUCE_NAME_ROUTE_MAX_VERSION: ApiVersion = ApiVersion::new(12);

/// Definitely-unsent rejection before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) enum ProduceSubmitError {
    InvalidTopic(TopicNameError),
    InvalidPartition(PartitionIdError),
    Driver(SubmitError),
}

impl ProduceSubmitError {
    /// Immediate submission failures have not crossed driver ownership.
    pub(crate) const fn delivery(&self) -> DeliveryStatus {
        match self {
            Self::InvalidTopic(_) | Self::InvalidPartition(_) | Self::Driver(_) => {
                DeliveryStatus::NotSent
            }
        }
    }

    /// Normalizes immediate rejection structure without inventing retry policy.
    pub(crate) const fn failure_kind(&self) -> ProducerAttemptFailureKind {
        match self {
            Self::InvalidTopic(_) | Self::InvalidPartition(_) => {
                ProducerAttemptFailureKind::Permanent
            }
            Self::Driver(SubmitError::Full) => ProducerAttemptFailureKind::LocalCapacity,
            Self::Driver(SubmitError::Wake(_)) => ProducerAttemptFailureKind::ConnectionUnavailable,
            Self::Driver(
                SubmitError::Closed
                | SubmitError::IdentityExhausted
                | SubmitError::ForeignDriver
                | SubmitError::VersionBoundsInvalid { .. },
            ) => ProducerAttemptFailureKind::Permanent,
        }
    }
}

impl fmt::Display for ProduceSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic(source) => write!(formatter, "invalid Produce topic: {source}"),
            Self::InvalidPartition(source) => {
                write!(formatter, "invalid Produce partition: {source}")
            }
            Self::Driver(source) => write!(formatter, "driver rejected Produce request: {source}"),
        }
    }
}

impl Error for ProduceSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTopic(source) => Some(source),
            Self::InvalidPartition(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    /// Submits one generated Produce owner with its original absolute deadline.
    ///
    /// The routed call retains its opaque route-failure token until the
    /// deterministic core later authorizes invalidation or deliberate discard.
    pub(crate) fn submit_tracked_produce(
        &self,
        topic: &str,
        partition: i32,
        request: ProduceRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ProduceResponse>, ProduceSubmitError> {
        let topic = TopicName::new(topic.to_owned()).map_err(ProduceSubmitError::InvalidTopic)?;
        let partition =
            PartitionId::new(partition).map_err(ProduceSubmitError::InvalidPartition)?;
        let route = Route::PartitionLeader { topic, partition };
        self.driver
            .request_tracked_with(route, request, produce_options(deadline))
            .map_err(ProduceSubmitError::Driver)
    }
}

pub(super) const fn produce_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Bulk)
        .with_maximum_version(PRODUCE_NAME_ROUTE_MAX_VERSION)
}
