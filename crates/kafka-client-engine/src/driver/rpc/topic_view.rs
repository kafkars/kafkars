//! Thin exact-topic adapter over the driver's immutable metadata projection.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{Call, SubmitError, TopicName, TopicNameError, TopicView, TopicViewError};

use super::super::DriverOwner;

/// Scalar driver-owned topic facts consumed by deterministic client policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TopicPartitionCountFact {
    pub(crate) metadata_generation: u64,
    pub(crate) logical_partition_count: u32,
}

/// One accepted immutable-view lookup under its original absolute deadline.
#[must_use = "an accepted topic-view lookup must settle or recover after driver shutdown"]
pub(crate) struct TopicPartitionCountCall {
    topic_view_topic: TopicName,
    topic_view_driver_call: Option<Call<Result<TopicView, TopicViewError>>>,
}

impl TopicPartitionCountCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        topic: &str,
        deadline: Instant,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(TopicPartitionCountAdmissionFailure::Topic)?;
        let call = driver
            .driver
            .topic_view(topic.clone(), deadline)
            .map_err(TopicPartitionCountAdmissionFailure::Driver)?;
        Ok(Self {
            topic_view_topic: topic,
            topic_view_driver_call: Some(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TopicPartitionCountFact, TopicPartitionCountFailure>> {
        let result = self.topic_view_driver_call.as_mut()?.try_result()?;
        drop(self.topic_view_driver_call.take());
        Some(match result {
            Err(_error) => Err(TopicPartitionCountFailure::Completion),
            Ok(Err(error)) => Err(normalize_error(error)),
            Ok(Ok(view)) if view.topic() != &self.topic_view_topic => {
                Err(TopicPartitionCountFailure::TopicMismatch)
            }
            Ok(Ok(view)) => Ok(TopicPartitionCountFact {
                metadata_generation: view.generation().get(),
                logical_partition_count: view.logical_partition_count(),
            }),
        })
    }

    pub(crate) fn discard_after_driver_shutdown(mut self) {
        drop(self.topic_view_driver_call.take());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopicPartitionCountFailure {
    Deadline,
    Unavailable,
    Refresh,
    Broker(i16),
    Malformed,
    Allocation,
    QueryCapacity(usize),
    Capacity {
        call_limit: usize,
        byte_limit: usize,
    },
    Draining,
    TopicMismatch,
    Completion,
}

#[derive(Debug)]
pub(crate) enum TopicPartitionCountAdmissionFailure {
    Topic(TopicNameError),
    Driver(SubmitError),
}

impl TopicPartitionCountAdmissionFailure {
    pub(crate) const fn kind(&self) -> TopicPartitionCountAdmissionFailureKind {
        match self {
            Self::Driver(SubmitError::Full) => TopicPartitionCountAdmissionFailureKind::Full,
            Self::Topic(_)
            | Self::Driver(
                SubmitError::Closed
                | SubmitError::Wake(_)
                | SubmitError::IdentityExhausted
                | SubmitError::ForeignDriver
                | SubmitError::VersionBoundsInvalid { .. },
            ) => TopicPartitionCountAdmissionFailureKind::Terminal,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopicPartitionCountAdmissionFailureKind {
    Full,
    Terminal,
}

impl fmt::Display for TopicPartitionCountAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic(source) => source.fmt(formatter),
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TopicPartitionCountAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Topic(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

const fn normalize_error(error: TopicViewError) -> TopicPartitionCountFailure {
    match error {
        TopicViewError::DeadlineExceeded => TopicPartitionCountFailure::Deadline,
        TopicViewError::Unavailable => TopicPartitionCountFailure::Unavailable,
        TopicViewError::RefreshFailed => TopicPartitionCountFailure::Refresh,
        TopicViewError::Broker { error_code } => TopicPartitionCountFailure::Broker(error_code),
        TopicViewError::MalformedMetadata => TopicPartitionCountFailure::Malformed,
        TopicViewError::ProjectionAllocationFailed => TopicPartitionCountFailure::Allocation,
        TopicViewError::QueryCapacityReached { limit } => {
            TopicPartitionCountFailure::QueryCapacity(limit)
        }
        TopicViewError::CapacityReached {
            call_limit,
            byte_limit,
        } => TopicPartitionCountFailure::Capacity {
            call_limit,
            byte_limit,
        },
        TopicViewError::Draining => TopicPartitionCountFailure::Draining,
    }
}
