//! Exact immutable route facts bound to one prepared broker-routed Fetch.

use core::num::NonZeroI16;

use kafka_client_core::{FetchFailure, partitioning::TopicMetadataGeneration};
use kafka_driver::{TopicName, TopicView, TopicViewError};

use super::{
    admission::PartitionFetchRequest,
    route::{BrokerFetchRouteFailure, BrokerId},
    topic_route::FetchTopicRoute,
};

/// One prepared request paired with exact immutable metadata authority.
#[must_use = "a broker-routed Fetch must be submitted or terminally settled"]
pub(crate) struct BrokerRoutedFetch {
    request: PartitionFetchRequest,
    broker_id: BrokerId,
}

impl BrokerRoutedFetch {
    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, BrokerId) {
        (self.request, self.broker_id)
    }
}

pub(super) fn correlate_view(
    mut request: PartitionFetchRequest,
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
    let Some((driver_broker_id, leader_epoch)) = (0..view.available_len()).find_map(|index| {
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
    let broker_id = BrokerId::from_driver(driver_broker_id);
    let route = FetchTopicRoute::observed(
        topic_id.to_bytes(),
        leader_epoch.map(|epoch| epoch.get()),
        TopicMetadataGeneration::from_raw(view.generation().get()),
    );
    if !request.bind_observed_topic_route(broker_id, route) {
        return Err(BrokerFetchRouteFailure::terminal(
            request,
            FetchFailure::Transport,
        ));
    }
    Ok(BrokerRoutedFetch { request, broker_id })
}

#[allow(
    clippy::match_same_arms,
    unreachable_patterns,
    reason = "the published driver RC exposes a non-exhaustive topic-view error while the reviewed path dependency is exhaustive"
)]
pub(super) fn topic_view_failure(
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
        _ => FetchFailure::DriverRejected,
    };
    BrokerFetchRouteFailure::terminal(request, failure)
}
