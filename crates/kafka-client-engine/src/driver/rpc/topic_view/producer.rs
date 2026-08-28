//! Routing policy borrowing over one accepted immutable driver topic view.

use std::time::Instant;

use kafka_client_core::{
    PartitionIndex,
    partitioning::{
        AvailablePartition, LeaderEpoch, PartitionCount, TopicMetadataGeneration,
        TopicPartitionSource,
    },
};
use kafka_driver::{
    Call, MetadataGeneration, RouteFailureToken, SubmitError, TopicName, TopicView, TopicViewError,
};

use super::super::super::DriverOwner;
use super::partition_count::{
    TopicPartitionCountAdmissionFailure, TopicPartitionCountFailure, normalize_error,
};

/// Ephemeral borrowed-policy adapter over one driver-owned immutable view.
#[derive(Debug)]
pub(crate) struct TopicRouteView {
    view: TopicView,
    logical_count: PartitionCount,
}

impl TopicRouteView {
    fn try_new(view: TopicView) -> Result<Self, TopicPartitionCountFailure> {
        let logical_count = PartitionCount::try_from_raw(view.logical_partition_count())
            .ok_or(TopicPartitionCountFailure::Malformed)?;
        let mut previous = None;
        for index in 0..view.available_len() {
            let fact = view
                .available_at(index)
                .ok_or(TopicPartitionCountFailure::Malformed)?;
            let partition = u32::try_from(fact.partition().get())
                .map_err(|_| TopicPartitionCountFailure::Malformed)?;
            if partition >= logical_count.get()
                || previous.is_some_and(|previous| partition <= previous)
            {
                return Err(TopicPartitionCountFailure::Malformed);
            }
            if let Some(epoch) = fact.leader_epoch() {
                LeaderEpoch::try_from_raw(epoch.get())
                    .map_err(|_| TopicPartitionCountFailure::Malformed)?;
            }
            previous = Some(partition);
        }
        Ok(Self {
            view,
            logical_count,
        })
    }

    /// Returns the exact driver-published leader identity for one partition.
    pub(crate) fn leader_broker_id(&self, partition: PartitionIndex) -> Option<i32> {
        let partition = i32::try_from(partition.get()).ok()?;
        (0..self.view.available_len()).find_map(|index| {
            self.view
                .available_at(index)
                .filter(|fact| fact.partition().get() == partition)
                .map(|fact| fact.broker_id().get())
        })
    }

    /// Returns the broker-issued UUID retained by this exact topic view.
    pub(crate) fn kafka_topic_id(&self) -> Option<[u8; 16]> {
        self.view
            .topic_id()
            .map(kafka_driver::KafkaTopicId::to_bytes)
    }

    /// Returns the exact driver generation from which this view was projected.
    pub(crate) const fn metadata_generation(&self) -> TopicMetadataGeneration {
        TopicMetadataGeneration::from_raw(self.view.generation().get())
    }
}

impl TopicPartitionSource for TopicRouteView {
    fn generation(&self) -> TopicMetadataGeneration {
        TopicMetadataGeneration::from_raw(self.view.generation().get())
    }

    fn logical_count(&self) -> PartitionCount {
        self.logical_count
    }

    fn available_len(&self) -> usize {
        self.view.available_len()
    }

    fn available_at(&self, index: usize) -> Option<AvailablePartition> {
        let fact = self.view.available_at(index)?;
        let partition = u32::try_from(fact.partition().get()).ok()?;
        let leader_epoch = match fact.leader_epoch() {
            Some(epoch) => LeaderEpoch::try_from_raw(epoch.get()).ok()?,
            None => None,
        };
        Some(AvailablePartition::new(
            PartitionIndex::from_raw(partition),
            leader_epoch,
        ))
    }
}

/// One engine-owned immutable-view lookup under the originating deadline.
#[must_use = "an accepted topic-route lookup must settle or recover"]
pub(crate) struct TopicRouteViewCall {
    topic_route_view_topic: TopicName,
    topic_route_view_driver_call: Option<Call<Result<TopicView, TopicViewError>>>,
}

/// Rejected causal topic refresh retaining its single-use routed-outcome token.
pub(crate) struct TopicRouteViewAfterFailureRejection {
    topic_route_view_retryable: bool,
    topic_route_view_failure_token: RouteFailureToken,
}

impl TopicRouteViewCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        topic: &str,
        deadline: Instant,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        Self::submit_inner(driver, topic, None, deadline)
    }

    pub(crate) fn submit_newer_than(
        driver: &DriverOwner,
        topic: &str,
        observed: TopicMetadataGeneration,
        deadline: Instant,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        Self::submit_inner(
            driver,
            topic,
            Some(MetadataGeneration::from_raw(observed.get())),
            deadline,
        )
    }

    pub(crate) fn submit_after_failure(
        driver: &DriverOwner,
        topic: &str,
        token: RouteFailureToken,
        deadline: Instant,
    ) -> Result<Self, TopicRouteViewAfterFailureRejection> {
        let topic = match TopicName::new(topic.to_owned()) {
            Ok(topic) => topic,
            Err(_error) => {
                return Err(TopicRouteViewAfterFailureRejection {
                    topic_route_view_retryable: false,
                    topic_route_view_failure_token: token,
                });
            }
        };
        let call = match driver
            .driver
            .topic_view_after_failure(topic.clone(), token, deadline)
        {
            Ok(call) => call,
            Err(error) => {
                let retryable = after_failure_rejection_is_retryable(error.reason());
                let (_reason, token) = error.into_parts();
                return Err(TopicRouteViewAfterFailureRejection {
                    topic_route_view_retryable: retryable,
                    topic_route_view_failure_token: token,
                });
            }
        };
        Ok(Self {
            topic_route_view_topic: topic,
            topic_route_view_driver_call: Some(call),
        })
    }

    fn submit_inner(
        driver: &DriverOwner,
        topic: &str,
        newer_than: Option<MetadataGeneration>,
        deadline: Instant,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(TopicPartitionCountAdmissionFailure::Topic)?;
        let call = match newer_than {
            None => driver.driver.topic_view(topic.clone(), deadline),
            Some(observed) => {
                driver
                    .driver
                    .topic_view_newer_than(topic.clone(), observed, deadline)
            }
        }
        .map_err(TopicPartitionCountAdmissionFailure::Driver)?;
        Ok(Self {
            topic_route_view_topic: topic,
            topic_route_view_driver_call: Some(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TopicRouteView, TopicPartitionCountFailure>> {
        let result = self.topic_route_view_driver_call.as_mut()?.try_result()?;
        drop(self.topic_route_view_driver_call.take());
        Some(match result {
            Err(_error) => Err(TopicPartitionCountFailure::Completion),
            Ok(Err(error)) => Err(normalize_error(error)),
            Ok(Ok(view)) if view.topic() != &self.topic_route_view_topic => {
                Err(TopicPartitionCountFailure::TopicMismatch)
            }
            Ok(Ok(view)) => TopicRouteView::try_new(view),
        })
    }

    pub(crate) fn discard_after_driver_shutdown(mut self) {
        drop(self.topic_route_view_driver_call.take());
    }

    /// Abandons observation while the live driver retains accepted lookup work.
    pub(crate) fn abandon(mut self) {
        if let Some(call) = self.topic_route_view_driver_call.take() {
            call.abandon();
        }
    }
}

pub(super) const fn after_failure_rejection_is_retryable(reason: &SubmitError) -> bool {
    matches!(reason, SubmitError::Full)
}

impl TopicRouteViewAfterFailureRejection {
    pub(crate) const fn is_retryable(&self) -> bool {
        self.topic_route_view_retryable
    }

    pub(crate) fn into_token(self) -> RouteFailureToken {
        self.topic_route_view_failure_token
    }
}
