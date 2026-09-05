//! Exact immutable route facts bound to one prepared broker-routed Fetch.

use core::num::NonZeroI16;

use kafka_client_core::FetchFailure;
use kafka_driver::{BrokerId as DriverBrokerId, TopicViewError};

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

pub(super) fn bind_route(
    mut request: PartitionFetchRequest,
    broker_id: DriverBrokerId,
    topic_id: [u8; 16],
    leader_epoch: Option<i32>,
    metadata_generation: kafka_client_core::partitioning::TopicMetadataGeneration,
) -> BrokerRoutedFetch {
    request.bind_topic_route(FetchTopicRoute::observed(
        topic_id,
        leader_epoch,
        metadata_generation,
    ));
    BrokerRoutedFetch {
        request,
        broker_id: BrokerId::from_driver(broker_id),
    }
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
