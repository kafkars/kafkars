//! Exact topic-view ownership resolving one prepared Fetch to its leader broker.

use core::num::NonZeroI16;

use kafka_client_core::FetchFailure;
use kafka_driver::{Call, CompletionError, SubmitError, TopicName, TopicView, TopicViewError};

use super::{super::super::DriverOwner, admission::PartitionFetchRequest};

/// One prepared request paired with exact immutable metadata authority.
#[must_use = "a broker-routed Fetch must be submitted or terminally settled"]
pub(crate) struct BrokerRoutedFetch {
    request: PartitionFetchRequest,
    broker_id: kafka_driver::BrokerId,
}

impl BrokerRoutedFetch {
    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, kafka_driver::BrokerId) {
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
            Err(source) => return Err(admission_failure(request, source)),
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
            Ok(Ok(view)) => correlate_view(request, &self.topic, view),
        })
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

fn correlate_view(
    request: PartitionFetchRequest,
    topic: &TopicName,
    view: TopicView,
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
    let broker_id = (0..view.available_len()).find_map(|index| {
        view.available_at(index)
            .filter(|entry| entry.partition().get() == partition)
            .map(|entry| entry.broker_id())
    });
    match broker_id {
        Some(broker_id) => Ok(BrokerRoutedFetch { request, broker_id }),
        None => Err(BrokerFetchRouteFailure::terminal(
            request,
            FetchFailure::Transport,
        )),
    }
}

fn admission_failure(
    request: PartitionFetchRequest,
    source: SubmitError,
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
