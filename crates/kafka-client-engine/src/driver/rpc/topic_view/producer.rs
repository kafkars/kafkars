//! Routing policy borrowing over one accepted immutable driver topic view.

use std::time::Instant;

use kafka_client_core::{
    PartitionIndex,
    partitioning::{
        AvailablePartition, LeaderEpoch, PartitionCount, TopicMetadataGeneration,
        TopicPartitionSource,
    },
};
use kafka_driver::{Call, MetadataGeneration, TopicName, TopicView, TopicViewError};

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
        let leader_epoch = fact
            .leader_epoch()
            .map(|epoch| LeaderEpoch::try_from_raw(epoch.get()))
            .transpose()
            .ok()?
            .flatten();
        Some(AvailablePartition::new(
            PartitionIndex::from_raw(partition),
            leader_epoch,
        ))
    }
}

/// One engine-owned immutable-view lookup under the originating deadline.
#[must_use = "an accepted topic-route lookup must settle or recover"]
pub(crate) struct TopicRouteViewCall {
    topic: TopicName,
    call: Option<Call<Result<TopicView, TopicViewError>>>,
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

    fn submit_inner(
        driver: &DriverOwner,
        topic: &str,
        newer_than: Option<MetadataGeneration>,
        deadline: Instant,
    ) -> Result<Self, TopicPartitionCountAdmissionFailure> {
        let topic =
            TopicName::new(topic.to_owned()).map_err(TopicPartitionCountAdmissionFailure::Topic)?;
        let call = newer_than
            .map_or_else(
                || driver.driver.topic_view(topic.clone(), deadline),
                |observed| {
                    driver
                        .driver
                        .topic_view_newer_than(topic.clone(), observed, deadline)
                },
            )
            .map_err(TopicPartitionCountAdmissionFailure::Driver)?;
        Ok(Self {
            topic,
            call: Some(call),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TopicRouteView, TopicPartitionCountFailure>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        Some(match result {
            Err(_error) => Err(TopicPartitionCountFailure::Completion),
            Ok(Err(error)) => Err(normalize_error(error)),
            Ok(Ok(view)) if view.topic() != &self.topic => {
                Err(TopicPartitionCountFailure::TopicMismatch)
            }
            Ok(Ok(view)) => TopicRouteView::try_new(view),
        })
    }

    pub(crate) fn discard_after_driver_shutdown(mut self) {
        drop(self.call.take());
    }
}
