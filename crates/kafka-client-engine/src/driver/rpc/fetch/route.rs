//! Exact topic-view ownership resolving one prepared Fetch to its leader broker.

use core::num::NonZeroI16;

#[cfg(test)]
use std::{error::Error, fmt};

use kafka_client_core::{AssignedConsumerEffect, FetchFailure};
use kafka_driver::{Call, CompletionError, SubmitError, TopicName, TopicView, TopicViewError};

use super::{super::super::DriverOwner, admission::PartitionFetchRequest};

/// Nonnegative Kafka broker identity retained by the broker-session owner.
///
/// The reviewed driver does not currently expose broker identities through
/// `TopicView`, so production routing cannot construct this value yet.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BrokerId(i32);

impl BrokerId {
    #[cfg(test)]
    pub(crate) const fn new(value: i32) -> Result<Self, BrokerIdError> {
        if value < 0 {
            return Err(BrokerIdError { value });
        }
        Ok(Self(value))
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BrokerIdError {
    value: i32,
}

#[cfg(test)]
impl fmt::Display for BrokerIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.value)
    }
}

#[cfg(test)]
impl Error for BrokerIdError {}

/// One prepared request paired with exact immutable metadata authority.
#[must_use = "a broker-routed Fetch must be submitted or terminally settled"]
#[allow(
    dead_code,
    reason = "the reviewed driver cannot yet project the broker identity needed to construct it"
)]
pub(crate) struct BrokerRoutedFetch {
    request: PartitionFetchRequest,
    broker_id: BrokerId,
}

impl BrokerRoutedFetch {
    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, BrokerId) {
        (self.request, self.broker_id)
    }
}

/// One accepted metadata lookup retaining its exact prepared Fetch.
#[must_use = "an accepted Fetch route lookup must settle or recover"]
pub(crate) struct BrokerFetchRouteCall {
    request: Option<PartitionFetchRequest>,
    topic: TopicName,
    call: Option<Call<Result<TopicView, TopicViewError>>>,
}

impl BrokerFetchRouteCall {
    #[allow(
        clippy::result_large_err,
        reason = "admission returns exact request ownership"
    )]
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PartitionFetchRequest,
    ) -> Result<Self, BrokerFetchRouteFailure> {
        let topic = match TopicName::new(request.topic().to_owned()) {
            Ok(topic) => topic,
            Err(_error) => {
                return Err(BrokerFetchRouteFailure::terminal(
                    request,
                    FetchFailure::DriverRejected,
                ));
            }
        };
        let call = match driver
            .driver
            .topic_view(topic.clone(), request.operation_deadline().transport())
        {
            Ok(call) => call,
            Err(source) => return Err(admission_failure(request, &source)),
        };
        Ok(Self {
            request: Some(request),
            topic,
            call: Some(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<BrokerRoutedFetch, BrokerFetchRouteFailure>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        let request = self.request.take()?;
        Some(match result {
            Err(source) => Err(completion_failure(request, source)),
            Ok(Err(source)) => Err(topic_view_failure(request, source)),
            Ok(Ok(view)) => correlate_view(request, &self.topic, &view),
        })
    }

    pub(crate) fn is_superseded_by(&self, effect: AssignedConsumerEffect) -> bool {
        self.request
            .as_ref()
            .is_some_and(|request| request.is_superseded_by(effect))
    }

    /// Abandons only `TopicView` observation and returns the definitely-unsent Fetch request.
    pub(crate) fn retire_for_control(mut self) -> PartitionFetchRequest {
        self.call
            .take()
            .unwrap_or_else(|| unreachable!("live route call retains completion observation"))
            .abandon();
        self.request
            .take()
            .unwrap_or_else(|| unreachable!("live route call retains its Fetch request"))
    }

    pub(crate) fn recover_after_driver_shutdown(mut self) -> PartitionFetchRequest {
        drop(self.call.take());
        self.request
            .take()
            .unwrap_or_else(|| panic!("unsettled route call retains its request"))
    }
}

/// Route-resolution failure retaining the exact prepared request.
#[must_use = "route failure ownership must be settled or recovered"]
pub(crate) struct BrokerFetchRouteFailure {
    request: PartitionFetchRequest,
    kind: BrokerFetchRouteFailureKind,
}

impl BrokerFetchRouteFailure {
    const fn terminal(request: PartitionFetchRequest, failure: FetchFailure) -> Self {
        Self {
            request,
            kind: BrokerFetchRouteFailureKind::Terminal(failure),
        }
    }

    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, BrokerFetchRouteFailureKind) {
        (self.request, self.kind)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerFetchRouteFailureKind {
    Backpressured,
    Terminal(FetchFailure),
    Completion,
}

#[allow(
    clippy::result_large_err,
    reason = "route failure returns the exact prepared Fetch request for deterministic settlement"
)]
fn correlate_view(
    request: PartitionFetchRequest,
    topic: &TopicName,
    view: &TopicView,
) -> Result<BrokerRoutedFetch, BrokerFetchRouteFailure> {
    if view.topic() != topic {
        return Err(BrokerFetchRouteFailure::terminal(
            request,
            FetchFailure::InvalidResponse,
        ));
    }
    let partition = request.fence().position().partition().partition().get();
    let partition = match i32::try_from(partition) {
        Ok(partition) => partition,
        Err(_error) => {
            return Err(BrokerFetchRouteFailure::terminal(
                request,
                FetchFailure::DriverRejected,
            ));
        }
    };
    let partition_is_available = (0..view.available_len()).any(|index| {
        view.available_at(index)
            .is_some_and(|entry| entry.partition().get() == partition)
    });
    let failure = if partition_is_available {
        // `TopicView` proves that a leader exists but does not expose its broker
        // identity. Do not substitute an arbitrary broker for exact routing.
        FetchFailure::DriverRejected
    } else {
        FetchFailure::Transport
    };
    Err(BrokerFetchRouteFailure::terminal(request, failure))
}

fn admission_failure(
    request: PartitionFetchRequest,
    source: &SubmitError,
) -> BrokerFetchRouteFailure {
    if matches!(source, SubmitError::Full) {
        BrokerFetchRouteFailure {
            request,
            kind: BrokerFetchRouteFailureKind::Backpressured,
        }
    } else {
        BrokerFetchRouteFailure::terminal(request, FetchFailure::DriverRejected)
    }
}

fn completion_failure(
    request: PartitionFetchRequest,
    _source: CompletionError,
) -> BrokerFetchRouteFailure {
    BrokerFetchRouteFailure {
        request,
        kind: BrokerFetchRouteFailureKind::Completion,
    }
}

fn topic_view_failure(
    request: PartitionFetchRequest,
    source: TopicViewError,
) -> BrokerFetchRouteFailure {
    let failure = match source {
        TopicViewError::DeadlineExceeded => FetchFailure::DeadlineElapsed,
        TopicViewError::Broker { error_code } => {
            NonZeroI16::new(error_code).map_or(FetchFailure::InvalidResponse, FetchFailure::Broker)
        }
        TopicViewError::MalformedMetadata => FetchFailure::InvalidResponse,
        TopicViewError::ProjectionAllocationFailed
        | TopicViewError::QueryCapacityReached { .. }
        | TopicViewError::CapacityReached { .. } => FetchFailure::DriverRejected,
        TopicViewError::Unavailable | TopicViewError::RefreshFailed | TopicViewError::Draining => {
            FetchFailure::Transport
        }
    };
    BrokerFetchRouteFailure::terminal(request, failure)
}
