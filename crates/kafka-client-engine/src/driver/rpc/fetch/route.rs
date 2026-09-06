//! Exact topic-view ownership resolving one prepared Fetch to its leader broker.
#![allow(
    clippy::redundant_closure_for_method_calls,
    clippy::result_large_err,
    reason = "route failures retain exact requests and kafka-driver hides its epoch type"
)]

use kafka_client_core::{
    AssignedConsumerEffect, FetchFailure, partitioning::TopicMetadataGeneration,
};
use kafka_driver::{
    BrokerId as DriverBrokerId, Call, MetadataGeneration, TopicName, TopicView, TopicViewError,
};

use super::{
    super::super::DriverOwner, admission::PartitionFetchRequest,
    route_correlation::BrokerRoutedFetch,
};

type BrokerFetchRouteResult = Result<BrokerRoutedFetch, BrokerFetchRouteFailure>;

/// Nonnegative Kafka broker identity retained by the broker-session owner.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BrokerId(DriverBrokerId);

impl BrokerId {
    #[cfg(test)]
    pub(crate) fn new(value: i32) -> Result<Self, kafka_driver::BrokerIdError> {
        DriverBrokerId::new(value).map(Self)
    }

    pub(crate) const fn from_driver(value: DriverBrokerId) -> Self {
        Self(value)
    }

    pub(crate) fn from_raw(value: i32) -> Result<Self, kafka_driver::BrokerIdError> {
        DriverBrokerId::new(value).map(Self)
    }

    pub(crate) const fn driver(self) -> DriverBrokerId {
        self.0
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
    pub(super) fn from_admitted(
        request: PartitionFetchRequest,
        topic: TopicName,
        call: Call<Result<TopicView, TopicViewError>>,
    ) -> Self {
        Self {
            request: Some(request),
            topic,
            call: Some(call),
        }
    }

    pub(crate) fn submit(
        driver: &DriverOwner,
        request: PartitionFetchRequest,
    ) -> Result<Self, BrokerFetchRouteFailure> {
        Self::submit_inner(driver, request, None)
    }

    pub(crate) fn submit_newer_than(
        driver: &DriverOwner,
        request: PartitionFetchRequest,
        observed: TopicMetadataGeneration,
    ) -> Result<Self, BrokerFetchRouteFailure> {
        Self::submit_inner(
            driver,
            request,
            Some(MetadataGeneration::from_raw(observed.get())),
        )
    }

    fn submit_inner(
        driver: &DriverOwner,
        request: PartitionFetchRequest,
        newer_than: Option<MetadataGeneration>,
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
        let deadline = request.operation_deadline().transport();
        let call = match newer_than.map_or_else(
            || driver.driver.topic_view(topic.clone(), deadline),
            |observed| {
                driver
                    .driver
                    .topic_view_newer_than(topic.clone(), observed, deadline)
            },
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(super::route_correlation::admit_failure(request, &source));
            }
        };
        Ok(Self::from_admitted(request, topic, call))
    }

    pub(crate) fn try_terminal(&mut self) -> Option<BrokerFetchRouteResult> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        let request = self.request.take()?;
        Some(match result {
            Err(source) => Err(super::route_correlation::completion_failure(
                request, source,
            )),
            Ok(Err(source)) => Err(super::route_correlation::topic_view_failure(
                request, source,
            )),
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
    let Some((broker_id, leader_epoch)) = (0..view.available_len()).find_map(|index| {
        view.available_at(index)
            .filter(|entry| entry.partition().get() == partition)
            .map(|entry| (entry.broker_id(), entry.leader_epoch()))
    }) else {
        return Err(BrokerFetchRouteFailure::terminal(
            request,
            FetchFailure::Transport,
        ));
    };
    let Some(topic_id) = view.topic_id() else {
        return Err(BrokerFetchRouteFailure::terminal(
            request,
            FetchFailure::Compatibility,
        ));
    };
    super::route_correlation::bind_route(
        request,
        broker_id,
        topic_id.to_bytes(),
        leader_epoch.map(|epoch| epoch.get()),
        TopicMetadataGeneration::from_raw(view.generation().get()),
    )
}

/// Route-resolution failure retaining the exact prepared request.
#[must_use = "route failure ownership must be settled or recovered"]
pub(crate) struct BrokerFetchRouteFailure {
    pub(super) request: PartitionFetchRequest,
    pub(super) kind: BrokerFetchRouteFailureKind,
}

impl BrokerFetchRouteFailure {
    pub(super) const fn terminal(request: PartitionFetchRequest, failure: FetchFailure) -> Self {
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
