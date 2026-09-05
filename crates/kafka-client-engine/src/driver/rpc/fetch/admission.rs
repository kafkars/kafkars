//! Exact prepared ownership and local admission for one partition Fetch.

use kafka_client_core::{AssignedConsumerEffect, FetchFence, NextFetchOffset};

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{
        FetchDecodeLimits, FetchIsolation, FetchRequestFailure, FetchRequestSettings,
        FetchSessionRequest,
    },
};

use super::{route::BrokerId, submission::FetchSubmitError, topic_route::FetchTopicRoute};

/// One core-selected Fetch paired with engine catalog, limits, and deadline facts.
#[must_use = "a prepared partition Fetch must be submitted or terminally settled"]
pub(crate) struct PartitionFetchRequest {
    fence: FetchFence,
    next_offset: NextFetchOffset,
    topic: String,
    topic_route: Option<FetchTopicRoute>,
    failed_broker: Option<BrokerId>,
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
    decode_limits: FetchDecodeLimits,
    operation_deadline: OperationDeadline,
}

impl PartitionFetchRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "the prepared Fetch owner carries every exact execution fact"
    )]
    pub(crate) fn from_fetch_ready_parts(
        fence: FetchFence,
        next_offset: NextFetchOffset,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        operation_deadline: OperationDeadline,
    ) -> Self {
        Self {
            fence,
            next_offset,
            topic,
            topic_route: None,
            failed_broker: None,
            settings,
            session: FetchSessionRequest::LEGACY,
            decode_limits,
            operation_deadline,
        }
    }

    pub(crate) fn from_effect(
        effect: AssignedConsumerEffect,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        operation_deadline: OperationDeadline,
    ) -> Result<Self, FetchRequestPreparationError> {
        let AssignedConsumerEffect::FetchReady { fence, next_offset } = effect else {
            return Err(FetchRequestPreparationError::UnexpectedEffect);
        };
        Ok(Self {
            fence,
            next_offset,
            topic,
            topic_route: None,
            failed_broker: None,
            settings,
            session: FetchSessionRequest::LEGACY,
            decode_limits,
            operation_deadline,
        })
    }

    pub(crate) const fn fence(&self) -> FetchFence {
        self.fence
    }

    pub(crate) fn is_superseded_by(&self, effect: AssignedConsumerEffect) -> bool {
        super::fence::supersedes(effect, self.fence)
    }

    pub(crate) const fn next_offset(&self) -> NextFetchOffset {
        self.next_offset
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn topic_route(&self) -> Option<FetchTopicRoute> {
        self.topic_route
    }

    pub(crate) fn topic_id(&self) -> Option<[u8; 16]> {
        self.topic_route.map(FetchTopicRoute::topic_id)
    }

    pub(crate) fn leader_epoch(&self) -> Option<i32> {
        self.topic_route.and_then(FetchTopicRoute::leader_epoch)
    }

    pub(crate) fn bind_topic_route(&mut self, route: FetchTopicRoute) {
        self.topic_route = Some(route);
        self.failed_broker = None;
    }

    pub(crate) fn bind_observed_topic_route(
        &mut self,
        broker_id: BrokerId,
        route: FetchTopicRoute,
    ) -> bool {
        self.topic_route = Some(route);
        self.failed_broker != Some(broker_id)
    }

    pub(crate) fn mark_failed_broker(&mut self, broker_id: BrokerId) {
        self.failed_broker = Some(broker_id);
    }

    #[cfg(test)]
    pub(crate) const fn failed_broker(&self) -> Option<BrokerId> {
        self.failed_broker
    }

    pub(crate) fn bind_cached_topic_route(
        &mut self,
        topic_id: [u8; 16],
        leader_epoch: Option<i32>,
        metadata_generation: Option<kafka_client_core::partitioning::TopicMetadataGeneration>,
    ) {
        self.topic_route = Some(metadata_generation.map_or_else(
            || FetchTopicRoute::new(topic_id, leader_epoch),
            |generation| FetchTopicRoute::observed(topic_id, leader_epoch, generation),
        ));
        self.failed_broker = None;
    }

    #[cfg(test)]
    pub(crate) fn bind_topic_route_for_test(
        &mut self,
        topic_id: [u8; 16],
        leader_epoch: Option<i32>,
    ) {
        self.bind_topic_route(FetchTopicRoute::new(topic_id, leader_epoch));
    }

    pub(crate) fn bind_retry(
        &mut self,
        fence: FetchFence,
        next_offset: NextFetchOffset,
        leader_epoch: Option<i32>,
    ) {
        self.fence = fence;
        self.next_offset = next_offset;
        if let (Some(route), Some(epoch)) = (self.topic_route, leader_epoch) {
            self.topic_route = Some(route.with_leader_epoch(epoch));
        }
        self.session = FetchSessionRequest::INITIAL;
    }

    pub(crate) fn clear_leader_epoch(&mut self) {
        self.topic_route = self.topic_route.map(FetchTopicRoute::without_leader_epoch);
    }

    pub(crate) const fn operation_deadline(&self) -> OperationDeadline {
        self.operation_deadline
    }

    pub(crate) const fn decode_limits(&self) -> FetchDecodeLimits {
        self.decode_limits
    }

    pub(crate) const fn isolation(&self) -> Option<FetchIsolation> {
        self.settings.isolation()
    }

    pub(super) const fn settings(&self) -> FetchRequestSettings {
        self.settings
    }

    pub(crate) const fn session(&self) -> FetchSessionRequest {
        self.session
    }

    pub(crate) fn bind_session(&mut self, session: FetchSessionRequest) {
        self.session = session;
    }
}

/// Preparation rejected a non-Fetch effect without consuming it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRequestPreparationError {
    UnexpectedEffect,
}

/// Definitely-unsent request construction or driver admission failure.
#[must_use = "the exact rejected Fetch request remains owned"]
pub(crate) struct FetchAdmissionFailure {
    request: PartitionFetchRequest,
    source: FetchAdmissionFailureSource,
}

impl FetchAdmissionFailure {
    pub(super) fn new(request: PartitionFetchRequest, source: FetchAdmissionFailureSource) -> Self {
        Self { request, source }
    }

    pub(super) fn deadline_elapsed(request: PartitionFetchRequest) -> Self {
        Self {
            request,
            source: FetchAdmissionFailureSource::DeadlineElapsed,
        }
    }

    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, FetchAdmissionFailureSource) {
        (self.request, self.source)
    }
}

/// Exact local boundary that rejected the prepared Fetch.
#[derive(Debug)]
pub(crate) enum FetchAdmissionFailureSource {
    DeadlineElapsed,
    EmptyBrokerBatch,
    InconsistentBrokerBatch,
    Request(FetchRequestFailure),
    Driver(FetchSubmitError),
}

/// Result of a capacity-preflighted attempt to submit one exact Fetch.
#[must_use = "backpressured or rejected Fetch ownership must be handled"]
pub(crate) enum FetchCallAdmission {
    Accepted,
    Backpressured(PartitionFetchRequest),
    Rejected(FetchAdmissionFailure),
}
